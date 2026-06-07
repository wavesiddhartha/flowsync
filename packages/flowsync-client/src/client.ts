import EventEmitter from "eventemitter3"
import { encode, decode, ClientMessage, ServerMessage, Meta } from "./protocol"
import { OfflineQueue } from "./offline"

export interface FlowSyncClientOptions {
  url:          string
  nodeId?:      string
  authToken?:   string
  offlineMode?: boolean          // default: true
  reconnect?:   boolean          // default: true
  maxRetryMs?:  number           // default: 30000
}

let WSConstructor: any = null

function generateUUID(): string {
  if (typeof crypto !== "undefined") {
    if (typeof crypto.randomUUID === "function") {
      return crypto.randomUUID()
    }
    if (typeof crypto.getRandomValues === "function") {
      const bytes = new Uint8Array(16)
      crypto.getRandomValues(bytes)
      bytes[6] = (bytes[6] & 0x0f) | 0x40
      bytes[8] = (bytes[8] & 0x3f) | 0x80
      const hex = Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('')
      return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
    }
  }
  return "node_" + Math.random().toString(36).substring(2, 15)
}

export class FlowSyncClient {
  nodeId: string
  private url: string
  private authToken: string
  private offlineMode: boolean
  private reconnect: boolean
  private maxRetryMs: number

  private ws: any = null
  private queue: OfflineQueue
  private subs = new Map<string, Set<(v: unknown, m: Meta) => void>>()
  private cache = new Map<string, any>()
  private cacheMeta = new Map<string, Meta>()
  private lastSeenTs: number = 0
  private retryMs: number = 2000
  private _isOnline: boolean = false
  private _explicitDisconnect: boolean = false
  private wasAuthenticated: boolean = false
  private pendingReplaysCount: number = 0
  private activeRooms = new Set<string>()
  private connectPromise: Promise<void> | null = null

  private heartbeatInterval: any = null
  private reconnectTimeout: any = null
  private authResolve: (() => void) | null = null
  private authReject: ((err: Error) => void) | null = null
  private pendingGets = new Map<string, { resolve: (val: any) => void; reject: (err: any) => void; timeout: any }>()
  private broadcastListeners = new Map<string, Set<(data: unknown) => void>>()
  private emitter = new EventEmitter()

  constructor(options: FlowSyncClientOptions) {
    this.url = options.url
    this.nodeId = options.nodeId || generateUUID()
    this.authToken = options.authToken || "anonymous"
    this.offlineMode = options.offlineMode !== false
    this.reconnect = options.reconnect !== false
    this.maxRetryMs = options.maxRetryMs || 30000
    this.queue = new OfflineQueue(this.nodeId)
  }

  // ── Connection ──────────────────────────────────────────

  connect(): Promise<void> {
    this._explicitDisconnect = false
    if (this._isOnline) return Promise.resolve()
    if (this.connectPromise) return this.connectPromise

    const initWS = (): Promise<void> => {
      if (WSConstructor) return Promise.resolve()
      if (typeof WebSocket !== "undefined") {
        WSConstructor = WebSocket
        return Promise.resolve()
      }
      return import("ws").then((wsModule) => {
        WSConstructor = wsModule.default || wsModule
      }).catch(() => {
        throw new Error(
          "FlowSyncClient: WebSocket constructor not found. Make sure to run in a browser or install the 'ws' package."
        )
      })
    }

    this.connectPromise = initWS().then(() => {
      return new Promise<void>((resolve, reject) => {
      this.authResolve = () => {
        this.connectPromise = null
        resolve()
      }
      this.authReject = (err: Error) => {
        this.connectPromise = null
        reject(err)
      }

      try {
        const ws = new WSConstructor(this.url)
        this.ws = ws

        ws.onopen = () => {
          try {
            console.log("CLIENT_CONNECT: onopen called. wasAuthenticated =", this.wasAuthenticated)
            if (this.wasAuthenticated) {
              this.handleReconnect()
            } else {
              const authMsg: ClientMessage = {
                type: "AUTH",
                token: this.authToken,
                node_id: this.nodeId
              }
              ws.send(encode(authMsg))
            }
            console.log("CLIENT_CONNECT: onopen finished successfully")
          } catch (err: any) {
            console.error("CLIENT_CONNECT: ERROR in onopen:", err.message, err.stack)
          }
        }

        ws.onmessage = (event: any) => {
          this.handleMessage(event.data)
        }

        ws.onerror = (event: any) => {
          const err = event.error || new Error("WebSocket error")
          this.emit("error", err)
          if (this.authReject) {
            this.authReject(err)
            this.authReject = null
            this.authResolve = null
          }
        }

        ws.onclose = () => {
          this.stopHeartbeat()
          this._isOnline = false
          this.ws = null
          this.emit("disconnect")

          if (this.authReject) {
            this.authReject(new Error("FlowSyncClient: connection closed before authentication completed."))
            this.authReject = null
            this.authResolve = null
          }

          if (this.reconnect && !this._explicitDisconnect) {
            this.startReconnectLoop()
          }
        }
      } catch (e) {
        this.connectPromise = null
        reject(e)
      }
    })
    })

    return this.connectPromise
  }

  async disconnect(): Promise<void> {
    const wasOnline = this._isOnline
    this._explicitDisconnect = true
    this._isOnline = false
    this.stopHeartbeat()
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout)
      this.reconnectTimeout = null
    }

    if (this.ws) {
      const socket = this.ws
      this.ws = null
      socket.onclose = null
      socket.close()
    }
    if (wasOnline) {
      this.emit("disconnect")
    }
  }

  get isOnline(): boolean {
    return this._isOnline
  }

  get pendingCount(): number {
    return this.queue.count
  }

  // ── State Operations ────────────────────────────────────

  async push(stream: string, value: unknown): Promise<void> {
    const ts = Date.now() / 1000

    const getSecureRandomHex = (): string => {
      if (typeof crypto !== "undefined" && typeof crypto.getRandomValues === "function") {
        const arr = new Uint8Array(4)
        crypto.getRandomValues(arr)
        return Array.from(arr, b => b.toString(16).padStart(2, '0')).join('')
      }
      return Math.random().toString(36).substring(2, 6)
    }

    // Enrich CRDT operations if missing op_id
    let enrichedValue = value
    if (Array.isArray(value)) {
      enrichedValue = value.map((item) => {
        if (item && typeof item === "object" && "op" in item && !("op_id" in item)) {
          return {
            ...item,
            op_id: `${this.nodeId}:${Date.now()}:${getSecureRandomHex()}`,
            ts: ts
          }
        }
        return item
      })
    } else if (value && typeof value === "object") {
      if ("op" in value && !("op_id" in value)) {
        enrichedValue = {
          ...value,
          op_id: `${this.nodeId}:${Date.now()}:${getSecureRandomHex()}`,
          ts: ts
        }
      }
    }

    if (this._isOnline) {
      try {
        this.sendRaw({
          type: "PUSH",
          stream,
          value: enrichedValue,
          ts
        })
        return
      } catch (err) {
        this.emit("error", err as Error)
      }
    }

    if (this.offlineMode) {
      this.queue.enqueue(stream, enrichedValue)
      // Optimistic local cache update for offline-first responsiveness
      this.applyIncomingUpdate(stream, enrichedValue, {
        ts,
        node_id: this.nodeId,
        merge_rule: this.cacheMeta.get(stream)?.mergeRule || "lww"
      })
    } else {
      throw new Error("Cannot push: Node is offline and offlineMode is disabled.")
    }
  }

  async get(stream: string): Promise<unknown> {
    if (this._isOnline && this.ws && this.ws.readyState === this.ws.OPEN) {
      const reqId = Math.random().toString(36).substring(2, 15)
      return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          this.pendingGets.delete(reqId)
          reject(new Error(`FlowSync: GET request for '${stream}' timed out.`))
        }, 5000)

        this.pendingGets.set(reqId, { resolve, reject, timeout })
        this.sendRaw({ type: "GET", stream, req_id: reqId })
      })
    }

    if (this.cache.has(stream)) {
      const val = this.cache.get(stream)
      const meta = this.cacheMeta.get(stream)
      return computeCrdtValue(val, meta?.mergeRule || "lww")
    }

    throw new Error(`FlowSync: stream '${stream}' not found in cache and client is offline.`)
  }

  // ── Subscriptions ───────────────────────────────────────

  on(stream: string, callback: (value: unknown, meta: Meta) => void): () => void {
    let callbacks = this.subs.get(stream)
    const isFirst = !callbacks
    if (!callbacks) {
      callbacks = new Set()
      this.subs.set(stream, callbacks)
    }
    callbacks.add(callback)

    if (this._isOnline && isFirst) {
      this.sendRaw({ type: "SUBSCRIBE", stream })
    }

    return () => this.off(stream, callback)
  }

  off(stream: string, callback: Function): void {
    const callbacks = this.subs.get(stream)
    if (callbacks) {
      callbacks.delete(callback as any)
      if (callbacks.size === 0) {
        this.subs.delete(stream)
        if (this._isOnline) {
          this.sendRaw({ type: "UNSUBSCRIBE", stream })
        }
      }
    }
  }

  // ── Rooms ───────────────────────────────────────────────

  joinRoom(room: string): void {
    this.activeRooms.add(room)
    if (this._isOnline) {
      this.sendRaw({ type: "JOIN_ROOM", room })
    }
  }

  leaveRoom(room: string): void {
    this.activeRooms.delete(room)
    if (this._isOnline) {
      this.sendRaw({ type: "LEAVE_ROOM", room })
    }
  }

  onBroadcast(event: string, callback: (data: unknown) => void): () => void {
    let listeners = this.broadcastListeners.get(event)
    if (!listeners) {
      listeners = new Set()
      this.broadcastListeners.set(event, listeners)
    }
    listeners.add(callback)
    return () => {
      const list = this.broadcastListeners.get(event)
      if (list) {
        list.delete(callback)
        if (list.size === 0) this.broadcastListeners.delete(event)
      }
    }
  }

  // ── Lifecycle Events ────────────────────────────────────

  onConnect(callback: () => void): () => void {
    this.emitter.on("connect", callback)
    return () => { this.emitter.off("connect", callback) }
  }

  onDisconnect(callback: () => void): () => void {
    this.emitter.on("disconnect", callback)
    return () => { this.emitter.off("disconnect", callback) }
  }

  onSyncComplete(callback: (s: SyncSummary) => void): () => void {
    this.emitter.on("sync:complete", callback)
    return () => { this.emitter.off("sync:complete", callback) }
  }

  onError(callback: (err: Error) => void): () => void {
    this.emitter.on("error", callback)
    return () => { this.emitter.off("error", callback) }
  }

  private emit(event: string, ...args: any[]): void {
    this.emitter.emit(event, ...args)
  }

  // ── Internal ────────────────────────────────────────────

  private handleMessage(raw: string): void {
    let msg: ServerMessage
    try {
      msg = decode(raw)
    } catch (err) {
      this.emit("error", err as Error)
      return
    }

    switch (msg.type) {
      case "AUTH_OK": {
        this.nodeId = msg.node_id
        this._isOnline = true
        this.wasAuthenticated = true
        this.retryMs = 2000
        this.emit("connect")
        this.startHeartbeat()

        for (const [stream, _] of this.subs) {
          this.sendRaw({ type: "SUBSCRIBE", stream })
        }
        for (const room of this.activeRooms) {
          this.sendRaw({ type: "JOIN_ROOM", room })
        }

        if (this.authResolve) {
          this.authResolve()
          this.authResolve = null
          this.authReject = null
        }
        break
      }
      case "AUTH_FAIL": {
        const err = new Error(`Authentication failed: ${msg.reason}`)
        this.emit("error", err)
        this.wasAuthenticated = false
        this.queue.clear()
        if (this.authReject) {
          this.authReject(err)
          this.authReject = null
          this.authResolve = null
        }
        this.disconnect()
        break
      }
      case "UPDATE": {
        this.lastSeenTs = Math.max(this.lastSeenTs, msg.ts)
        this.applyIncomingUpdate(msg.stream, msg.value, {
          ts: msg.ts,
          node_id: msg.node_id,
          merge_rule: msg.metadata?.merge_rule
        })
        break
      }
      case "GET_REPLY": {
        const pending = this.pendingGets.get(msg.req_id)
        if (pending) {
          clearTimeout(pending.timeout)
          this.pendingGets.delete(msg.req_id)

          const mergeRule = this.cacheMeta.get(msg.stream)?.mergeRule || "lww"
          const finalMeta: Meta = {
            nodeId: msg.node_id || "server",
            timestamp: msg.ts || Date.now() / 1000,
            stream: msg.stream,
            mergeRule
          }
          this.cache.set(msg.stream, msg.value)
          this.cacheMeta.set(msg.stream, finalMeta)

          pending.resolve(computeCrdtValue(msg.value, mergeRule))
        }
        break
      }
      case "RECONNECT_ACK": {
        this._isOnline = true
        this.emit("connect")

        for (const [stream, _] of this.subs) {
          this.sendRaw({ type: "SUBSCRIBE", stream })
        }
        for (const room of this.activeRooms) {
          this.sendRaw({ type: "JOIN_ROOM", room })
        }

        const missedStreams = new Set<string>()
        for (const update of msg.missed_updates) {
          this.lastSeenTs = Math.max(this.lastSeenTs, update.ts)
          missedStreams.add(update.stream)
          this.applyIncomingUpdate(update.stream, update.value, {
            ts: update.ts,
            node_id: update.node_id,
            merge_rule: update.metadata?.merge_rule
          })
        }

        const pendingChanges = this.queue.drain()
        const replayedChanges = pendingChanges.slice(0, this.pendingReplaysCount)
        const pendingStreams = new Set<string>()
        for (const change of replayedChanges) {
          pendingStreams.add(change.stream)
          const mergeRule = this.cacheMeta.get(change.stream)?.mergeRule || "lww"
          this.applyIncomingUpdate(change.stream, change.value, {
            ts: change.timestamp,
            node_id: this.nodeId,
            merge_rule: mergeRule
          })
        }

        this.queue.removeProgress(this.pendingReplaysCount)
        const sentCount = this.pendingReplaysCount
        this.pendingReplaysCount = 0

        const syncedStreams = Array.from(new Set([...missedStreams, ...pendingStreams]))
        const conflictsResolved = msg.missed_updates.filter((u) => u.metadata?.conflict_resolved).length

        const summary: SyncSummary = {
          syncedStreams,
          conflictsResolved,
          changesSent: sentCount,
          changesReceived: msg.missed_updates.length
        }

        this.startHeartbeat()
        if (this.authResolve) {
          this.authResolve()
          this.authResolve = null
          this.authReject = null
        }

        this.emit("sync:complete", summary)
        break
      }
      case "BROADCAST": {
        const listeners = this.broadcastListeners.get(msg.event)
        if (listeners) {
          listeners.forEach((cb) => {
            try {
              cb(msg.data)
            } catch (err) {
              this.emit("error", err as Error)
            }
          })
        }
        break
      }
      case "ERROR": {
        this.emit("error", new Error(`FlowSync Server Error [code=${msg.code}]: ${msg.message}`))
        break
      }
      case "PONG": {
        break
      }
    }
  }

  private handleReconnect(): void {
    const pendingChanges = this.queue.drain()
    this.pendingReplaysCount = pendingChanges.length
    this.sendRaw({
      type: "RECONNECT",
      node_id: this.nodeId,
      token: this.authToken,
      last_seen_ts: this.lastSeenTs,
      pending: pendingChanges
    })
  }

  private startReconnectLoop(): void {
    if (this.reconnectTimeout) return

    this.reconnectTimeout = setTimeout(async () => {
      this.reconnectTimeout = null
      if (this._isOnline || this._explicitDisconnect) return

      try {
        await this.connect()
      } catch (err) {
        this.emit("error", err as Error)
        this.retryMs = Math.min(this.retryMs * 2, this.maxRetryMs)
        this.startReconnectLoop()
      }
    }, this.retryMs)
  }

  private sendRaw(msg: ClientMessage): void {
    if (!this.ws || this.ws.readyState !== this.ws.OPEN) {
      throw new Error("FlowSyncClient: not connected to Hub")
    }
    this.ws.send(encode(msg))
  }

  private startHeartbeat(): void {
    this.stopHeartbeat()
    this.heartbeatInterval = setInterval(() => {
      if (this._isOnline && this.ws && this.ws.readyState === this.ws.OPEN) {
        try {
          this.sendRaw({ type: "PING" })
        } catch {
          // Handled by connection closing
        }
      }
    }, 30000)
  }

  private stopHeartbeat(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval)
      this.heartbeatInterval = null
    }
  }

  private applyIncomingUpdate(streamName: string, val: any, meta: any): void {
    const mergeRule = this.cacheMeta.get(streamName)?.mergeRule || meta.merge_rule || "lww"
    const existingVal = this.cache.get(streamName)
    const existingMeta: Meta = this.cacheMeta.get(streamName) || { timestamp: 0, nodeId: "system", stream: streamName }

    let mergedVal = val
    let mergedMeta = meta

    const mappedMetaA = {
      ts: existingMeta.timestamp,
      node_id: existingMeta.nodeId
    }

    if (existingMeta.timestamp > 0) {
      let mergeFn: any
      if (mergeRule === "crdt-counter") mergeFn = crdtCounterMerge
      else if (mergeRule === "crdt-set") mergeFn = crdtSetMerge
      else if (mergeRule === "crdt-text") mergeFn = crdtTextMerge
      else if (mergeRule === "append") mergeFn = appendMerge
      else if (mergeRule === "min") mergeFn = minMerge
      else if (mergeRule === "max") mergeFn = maxMerge
      else mergeFn = lwwMerge

      const [mVal, mMeta] = mergeFn(existingVal, val, mappedMetaA, meta)
      mergedVal = mVal
      mergedMeta = mMeta
    } else {
      if (mergeRule === "crdt-counter") {
        const [initVal] = crdtCounterMerge(null, val, null, meta)
        mergedVal = initVal
      } else if (mergeRule === "crdt-set") {
        const [initVal] = crdtSetMerge(null, val, null, meta)
        mergedVal = initVal
      } else if (mergeRule === "crdt-text") {
        const [initVal] = crdtTextMerge(null, val, null, meta)
        mergedVal = initVal
      }
    }

    const finalMeta: Meta = {
      nodeId: mergedMeta.node_id || mergedMeta.nodeId || "system",
      timestamp: mergedMeta.ts || mergedMeta.timestamp || 0,
      stream: streamName,
      mergeRule: mergeRule
    }

    this.cache.set(streamName, mergedVal)
    this.cacheMeta.set(streamName, finalMeta)

    const computed = computeCrdtValue(mergedVal, mergeRule)

    const callbacks = this.subs.get(streamName)
    if (callbacks) {
      callbacks.forEach((cb) => {
        try {
          cb(computed, finalMeta)
        } catch (err) {
          this.emit("error", err as Error)
        }
      })
    }
  }
}

// ── Merge Rules Helpers ────────────────────────────────────

function lwwMerge(valA: any, valB: any, metaA: any, metaB: any): [any, any] {
  const tsA = metaA?.ts || 0
  const tsB = metaB?.ts || 0
  const nodeA = metaA?.node_id || ""
  const nodeB = metaB?.node_id || ""
  if (tsB > tsA || (tsB === tsA && nodeB > nodeA)) {
    return [valB, metaB]
  }
  return [valA, metaA]
}

function crdtCounterMerge(valA: any, valB: any, metaA: any, metaB: any): [any, any] {
  const stateA = (valA && typeof valA === "object" && "increments" in valA)
    ? valA
    : { increments: {}, decrements: {} }
  const ts = Math.max(metaA?.ts || 0, metaB?.ts || 0)
  const nodeIdB = metaB?.node_id || "server"

  if (valB && typeof valB === "object" && "op" in valB) {
    const state = {
      increments: { ...stateA.increments },
      decrements: { ...stateA.decrements }
    }
    const op = valB.op
    const amount = valB.amount ?? 1
    if (op === "increment") {
      state.increments[nodeIdB] = (state.increments[nodeIdB] ?? 0) + amount
    } else if (op === "decrement") {
      state.decrements[nodeIdB] = (state.decrements[nodeIdB] ?? 0) + amount
    }
    return [state, { ...metaB, ts }]
  } else {
    const stateB = (valB && typeof valB === "object" && "increments" in valB)
      ? valB
      : { increments: {}, decrements: {} }
    const merged = { increments: { ...stateA.increments }, decrements: { ...stateA.decrements } }

    for (const [k, v] of Object.entries(stateB.increments || {})) {
      merged.increments[k] = Math.max(merged.increments[k] ?? 0, v as number)
    }
    for (const [k, v] of Object.entries(stateB.decrements || {})) {
      merged.decrements[k] = Math.max(merged.decrements[k] ?? 0, v as number)
    }
    return [merged, { ...metaB, ts }]
  }
}

function crdtSetMerge(valA: any, valB: any, metaA: any, metaB: any): [any, any] {
  const stateA = (valA && typeof valA === "object" && "added" in valA)
    ? valA
    : { added: [], removed: [] }
  const ts = Math.max(metaA?.ts || 0, metaB?.ts || 0)

  if (valB && typeof valB === "object" && "op" in valB) {
    const state = {
      added: [...stateA.added],
      removed: [...stateA.removed]
    }
    const op = valB.op
    const item = valB.item
    if (op === "add") {
      if (!state.added.includes(item)) state.added.push(item)
    } else if (op === "remove") {
      if (!state.removed.includes(item)) state.removed.push(item)
    }
    return [state, { ...metaB, ts }]
  } else {
    const stateB = (valB && typeof valB === "object" && "added" in valB)
      ? valB
      : { added: [], removed: [] }
    const added = Array.from(new Set([...stateA.added, ...stateB.added]))
    const removed = Array.from(new Set([...stateA.removed, ...stateB.removed]))
    return [{ added, removed }, { ...metaB, ts }]
  }
}

function crdtTextMerge(valA: any, valB: any, metaA: any, metaB: any): [any, any] {
  const listA = Array.isArray(valA) ? valA : []
  const listB = Array.isArray(valB) ? valB : (valB && typeof valB === "object" && "op" in valB ? [valB] : [])

  let merged = mergeOps(listA, listB)
  const ts = Math.max(metaA?.ts || 0, metaB?.ts || 0)
  
  // ── SECURITY: Compact if ops exceed limit ──
  const MAX_CRDT_TEXT_OPS = 50000
  if (merged.length > MAX_CRDT_TEXT_OPS) {
    const finalText = applyAllText(merged)
    merged = [{
      op: "insert",
      pos: 0,
      char: finalText,
      op_id: `compact:${Date.now()}`,
      ts: ts
    }]
  }

  return [merged, { ...metaB, ts }]
}

function mergeOps(opsA: any[], opsB: any[]): any[] {
  const seen = new Set<string>()
  const merged: any[] = []

  for (const op of [...opsA, ...opsB]) {
    if (op && typeof op === "object" && op.op_id) {
      if (!seen.has(op.op_id)) {
        seen.add(op.op_id)
        merged.push({ ...op })
      }
    }
  }

  merged.sort((a, b) => {
    const tsA = a.ts || 0
    const tsB = b.ts || 0
    if (tsA !== tsB) return tsA - tsB
    const idA = a.op_id || ""
    const idB = b.op_id || ""
    if (idA < idB) return -1
    if (idA > idB) return 1
    return 0
  })

  // Count unique nodes to see if position adjustment is necessary
  const uniqueNodes = new Set<string>()
  for (const op of merged) {
    const node = (op.op_id || "").split(":")[0] || ""
    uniqueNodes.add(node)
  }

  if (uniqueNodes.size > 1) {
    for (let i = 0; i < merged.length; i++) {
      const opI = merged[i]
      let posI = opI.pos || 0
      const nodeI = (opI.op_id || "").split(":")[0] || ""

      for (let j = 0; j < i; j++) {
        const opJ = merged[j]
        const nodeJ = (opJ.op_id || "").split(":")[0] || ""

        if (nodeI !== nodeJ) {
          if (opJ.op === "insert") {
            if ((opJ.pos || 0) <= posI) {
              posI += (opJ.char || "").length
            }
          } else if (opJ.op === "delete") {
            const len = opJ.len ?? 1
            if ((opJ.pos || 0) < posI) {
              posI = Math.max(0, posI - len)
            }
          }
        }
      }
      opI.pos = posI
    }
  }
  return merged
}

function applyAllText(ops: any[]): string {
  const chars: string[] = []
  for (const op of ops) {
    if (!op || typeof op !== "object") continue
    const opType = op.op
    let pos = op.pos || 0

    if (opType === "insert") {
      const val = op.char || ""
      pos = Math.max(0, Math.min(pos, chars.length))
      if (pos === chars.length) {
        for (let j = 0; j < val.length; j++) {
          chars.push(val[j])
        }
      } else {
        chars.splice(pos, 0, ...val)
      }
    } else if (opType === "delete") {
      const len = op.len ?? 1
      pos = Math.max(0, Math.min(pos, chars.length - 1))
      if (chars.length > 0) {
        chars.splice(pos, len)
      }
    }
  }
  return chars.join("")
}

function appendMerge(valA: any, valB: any, metaA: any, metaB: any): [any, any] {
  const listA = Array.isArray(valA) ? valA : (valA !== undefined && valA !== null ? [valA] : [])
  const listB = Array.isArray(valB) ? valB : (valB !== undefined && valB !== null ? [valB] : [])
  const ts = Math.max(metaA?.ts || 0, metaB?.ts || 0)
  return [[...listA, ...listB], { ...metaB, ts }]
}

function minMerge(valA: any, valB: any, metaA: any, metaB: any): [any, any] {
  if (valA === undefined || valA === null) return [valB, metaB]
  if (valB === undefined || valB === null) return [valA, metaA]
  return valB < valA ? [valB, metaB] : [valA, metaA]
}

function maxMerge(valA: any, valB: any, metaA: any, metaB: any): [any, any] {
  if (valA === undefined || valA === null) return [valB, metaB]
  if (valB === undefined || valB === null) return [valA, metaA]
  return valB > valA ? [valB, metaB] : [valA, metaA]
}

function computeCrdtValue(val: any, mergeRule: string): any {
  if (mergeRule === "crdt-counter") {
    if (val && typeof val === "object") {
      const inc = Object.values(val.increments || {}).reduce((acc: number, v: any) => acc + (v as number), 0)
      const dec = Object.values(val.decrements || {}).reduce((acc: number, v: any) => acc + (v as number), 0)
      return inc - dec
    }
    return val ?? 0
  }
  if (mergeRule === "crdt-set") {
    if (val && typeof val === "object") {
      const added = new Set(val.added || [])
      const removed = new Set(val.removed || [])
      return Array.from(added).filter((item) => !removed.has(item))
    }
    return val ?? []
  }
  if (mergeRule === "crdt-text") {
    if (Array.isArray(val)) {
      return applyAllText(val)
    }
    return val ?? ""
  }
  return val
}

export interface SyncSummary {
  syncedStreams:      string[]
  conflictsResolved: number
  changesSent:       number
  changesReceived:   number
}

export type { Meta } from "./protocol"
