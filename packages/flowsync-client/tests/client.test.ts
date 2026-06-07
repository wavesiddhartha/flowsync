import { describe, it, expect, beforeEach, afterEach, vi } from "vitest"
import { FlowSyncClient } from "../src/client"
import { ClientMessage, ServerMessage, encode, decode } from "../src/protocol"

// Stub localStorage globally for the client test file
const store: Record<string, string> = {}
const mockLocalStorage = {
  getItem: vi.fn((key: string) => store[key] || null),
  setItem: vi.fn((key: string, value: string) => {
    store[key] = value.toString()
  }),
  removeItem: vi.fn((key: string) => {
    delete store[key]
  }),
  clear: vi.fn(() => {
    for (const k of Object.keys(store)) {
      delete store[k]
    }
  })
}
vi.stubGlobal("localStorage", mockLocalStorage)


// Simple Mock WebSocket implementation
class MockWebSocket {
  url: string
  readyState: number
  onopen: (() => void) | null = null
  onclose: (() => void) | null = null
  onmessage: (event: { data: string }) => void = () => {}
  onerror: (event: any) => void = () => {}
  
  static instances: MockWebSocket[] = []
  
  OPEN = 1
  CLOSED = 3
  
  constructor(url: string) {
    this.url = url
    this.readyState = 1 // OPEN
    MockWebSocket.instances.push(this)
    console.log("MOCK_WS: Created new instance. Total instances =", MockWebSocket.instances.length)
    // Trigger onopen in next tick
    setTimeout(() => {
      console.log("MOCK_WS: Triggering onopen. handler defined =", typeof this.onopen === "function")
      if (typeof this.onopen === "function") {
        this.onopen()
      }
    }, 0)
  }
  
  send = vi.fn()
  close = vi.fn(() => {
    this.readyState = 3 // CLOSED
    setTimeout(() => {
      if (typeof this.onclose === "function") {
        this.onclose()
      }
    }, 0)
  })
}

// Stub WebSocket globally
vi.stubGlobal("WebSocket", MockWebSocket)

describe("FlowSyncClient Core Tests", () => {
  let clients: FlowSyncClient[] = []

  beforeEach(() => {
    clients = []
    MockWebSocket.instances = []
    if (typeof localStorage !== "undefined") {
      localStorage.clear()
    }
  })

  afterEach(async () => {
    for (const client of clients) {
      try {
        await client.disconnect()
      } catch (e) {}
    }
  })

  it("should connect and authenticate successfully", async () => {
    const client = new FlowSyncClient({
      url: "ws://localhost:8765",
      authToken: "test-token",
      nodeId: "alice"
    })
    clients.push(client)

    const connectPromise = client.connect()

    // Give connection handshake time to start
    await new Promise(r => setTimeout(r, 10))

    const wsInstance = MockWebSocket.instances[0]
    expect(wsInstance).toBeDefined()

    // Assert AUTH message was sent
    expect(wsInstance.send).toHaveBeenCalled()
    const sentMsg = JSON.parse(wsInstance.send.mock.calls[0][0])
    expect(sentMsg.type).toBe("AUTH")
    expect(sentMsg.token).toBe("test-token")
    expect(sentMsg.node_id).toBe("alice")

    // Simulate AUTH_OK reply
    const authOk: ServerMessage = { type: "AUTH_OK", node_id: "alice" }
    wsInstance.onmessage({ data: JSON.stringify(authOk) })

    await connectPromise

    expect(client.isOnline).toBe(true)
    expect(client.nodeId).toBe("alice")
  })

  it("should push message to WebSocket when online", async () => {
    const client = new FlowSyncClient({
      url: "ws://localhost:8765",
      nodeId: "alice"
    })
    clients.push(client)

    const connectPromise = client.connect()
    await new Promise(r => setTimeout(r, 10))
    const wsInstance = MockWebSocket.instances[0]
    wsInstance.onmessage({ data: JSON.stringify({ type: "AUTH_OK", node_id: "alice" }) })
    await connectPromise

    wsInstance.send.mockClear()
    await client.push("game:score", 42)

    expect(wsInstance.send).toHaveBeenCalled()
    const sentMsg = JSON.parse(wsInstance.send.mock.calls[0][0])
    expect(sentMsg.type).toBe("PUSH")
    expect(sentMsg.stream).toBe("game:score")
    expect(sentMsg.value).toBe(42)
  })

  it("should queue push locally in OfflineQueue when offline", async () => {
    const client = new FlowSyncClient({
      url: "ws://localhost:8765",
      nodeId: "bob"
    })
    clients.push(client)

    expect(client.isOnline).toBe(false)
    await client.push("game:score", 100)

    expect(MockWebSocket.instances.length).toBe(0)
    expect(client.pendingCount).toBe(1)
  })

  it("should subscribe to stream updates and receive messages", async () => {
    const client = new FlowSyncClient({
      url: "ws://localhost:8765",
      nodeId: "alice"
    })
    clients.push(client)

    const connectPromise = client.connect()
    await new Promise(r => setTimeout(r, 10))
    const wsInstance = MockWebSocket.instances[0]
    wsInstance.onmessage({ data: JSON.stringify({ type: "AUTH_OK", node_id: "alice" }) })
    await connectPromise

    wsInstance.send.mockClear()
    let lastValue: any = null
    client.on("game:score", (v) => {
      lastValue = v
    })

    // Assert SUBSCRIBE message sent
    expect(wsInstance.send).toHaveBeenCalled()
    const subscribeMsg = JSON.parse(wsInstance.send.mock.calls[0][0])
    expect(subscribeMsg.type).toBe("SUBSCRIBE")
    expect(subscribeMsg.stream).toBe("game:score")

    // Simulate UPDATE message
    const updateMsg: ServerMessage = {
      type: "UPDATE",
      stream: "game:score",
      value: 125,
      ts: Date.now() / 1000,
      node_id: "server"
    }
    wsInstance.onmessage({ data: JSON.stringify(updateMsg) })

    expect(lastValue).toBe(125)
  })

  it("should support unsubscribing callbacks", async () => {
    const client = new FlowSyncClient({
      url: "ws://localhost:8765",
      nodeId: "alice"
    })
    clients.push(client)

    const connectPromise = client.connect()
    await new Promise(r => setTimeout(r, 10))
    const wsInstance = MockWebSocket.instances[0]
    wsInstance.onmessage({ data: JSON.stringify({ type: "AUTH_OK", node_id: "alice" }) })
    await connectPromise

    let callCount = 0
    const unsub = client.on("game:score", () => {
      callCount++
    })

    wsInstance.onmessage({
      data: JSON.stringify({
        type: "UPDATE",
        stream: "game:score",
        value: 10,
        ts: Date.now() / 1000,
        node_id: "server"
      })
    })
    expect(callCount).toBe(1)

    unsub() // Unsubscribe

    wsInstance.onmessage({
      data: JSON.stringify({
        type: "UPDATE",
        stream: "game:score",
        value: 20,
        ts: Date.now() / 1000,
        node_id: "server"
      })
    })
    expect(callCount).toBe(1) // unchanged
  })

  it("should send GET request and resolve value on GET_REPLY", async () => {
    const client = new FlowSyncClient({
      url: "ws://localhost:8765",
      nodeId: "alice"
    })
    clients.push(client)

    const connectPromise = client.connect()
    await new Promise(r => setTimeout(r, 10))
    const wsInstance = MockWebSocket.instances[0]
    wsInstance.onmessage({ data: JSON.stringify({ type: "AUTH_OK", node_id: "alice" }) })
    await connectPromise

    wsInstance.send.mockClear()
    const getPromise = client.get("game:score")

    // Verify GET message sent
    expect(wsInstance.send).toHaveBeenCalled()
    const getMsg = JSON.parse(wsInstance.send.mock.calls[0][0])
    expect(getMsg.type).toBe("GET")
    expect(getMsg.stream).toBe("game:score")
    const reqId = getMsg.req_id
    expect(reqId).toBeDefined()

    // Simulate GET_REPLY
    const getReply: ServerMessage = {
      type: "GET_REPLY",
      stream: "game:score",
      value: 99,
      req_id: reqId
    }
    wsInstance.onmessage({ data: JSON.stringify(getReply) })

    const res = await getPromise
    expect(res).toBe(99)
  })

  it("should send RECONNECT and replay pending changes when reconnected", async () => {
    console.log("RECONNECT_TEST: 1. Starting test")
    // 1. Push offline changes
    const client = new FlowSyncClient({
      url: "ws://localhost:8765",
      nodeId: "alice"
    })
    clients.push(client)
    await client.push("doc:text", 10)
    await client.push("doc:text", 20)
    expect(client.pendingCount).toBe(2)
    console.log("RECONNECT_TEST: 2. Pushed offline changes, pendingCount =", client.pendingCount)

    // Set wasAuthenticated flag to force RECONNECT instead of AUTH
    client["wasAuthenticated"] = true

    // 2. Connect client
    console.log("RECONNECT_TEST: 3. Calling client.connect()")
    const connectPromise = client.connect()
    console.log("RECONNECT_TEST: 4. Waiting 10ms for handshake...")
    await new Promise<void>(resolve => {
      setTimeout(() => {
        console.log("RECONNECT_TEST: Timeout callback executed")
        resolve()
      }, 10)
    })
    console.log("RECONNECT_TEST: After handshake wait")
    const wsInstance = MockWebSocket.instances[0]
    console.log("RECONNECT_TEST: 5. Handshake wait done, wsInstance defined =", !!wsInstance)

    // Verify RECONNECT message sent
    expect(wsInstance.send).toHaveBeenCalled()
    const reconnectMsg = JSON.parse(wsInstance.send.mock.calls[0][0])
    console.log("RECONNECT_TEST: 6. Sent message type =", reconnectMsg.type)
    expect(reconnectMsg.type).toBe("RECONNECT")
    expect(reconnectMsg.node_id).toBe("alice")
    expect(reconnectMsg.pending.length).toBe(2)

    // Simulate RECONNECT_ACK
    console.log("RECONNECT_TEST: 7. Simulating RECONNECT_ACK")
    wsInstance.onmessage({
      data: JSON.stringify({
        type: "RECONNECT_ACK",
        missed_updates: []
      })
    })

    console.log("RECONNECT_TEST: 8. Awaiting connectPromise...")
    await connectPromise
    console.log("RECONNECT_TEST: 9. connectPromise resolved! pendingCount =", client.pendingCount, "isOnline =", client.isOnline)
    expect(client.pendingCount).toBe(0)
    expect(client.isOnline).toBe(true)
  })

  it("should overwrite crdt-text cache when receiving a server-coordinated snapshot", () => {
    const client = new FlowSyncClient({
      url: "ws://localhost:8765",
      nodeId: "alice"
    })
    clients.push(client)
    
    // Register stream as crdt-text
    client["cacheMeta"].set("doc:crdt", { mergeRule: "crdt-text", nodeId: "alice", timestamp: Date.now() / 1000 - 10 })
    
    // Generate 50,005 insert operations of character 'a'
    const ops = Array.from({ length: 50005 }, (_, i) => ({
      op: "insert",
      pos: i,
      char: "a",
      op_id: `alice:${Date.now()}:${i}`,
      ts: Date.now() / 1000 - 5
    }))
    
    // Set initial cache
    client["cache"].set("doc:crdt", ops)
    
    // Server sends a compacted snapshot
    const compactedSnapshot = [{
      op: "insert",
      pos: 0,
      char: "a".repeat(50005) + "b",
      op_id: "compact:12345",
      ts: Date.now() / 1000
    }]
    
    client["applyIncomingUpdate"]("doc:crdt", compactedSnapshot, {
      ts: Date.now() / 1000,
      node_id: "server",
      merge_rule: "crdt-text",
      snapshot: true // server-coordinated snapshot flag
    })
    
    const cached = client["cache"].get("doc:crdt") as any[]
    expect(cached.length).toBe(1)
    expect(cached[0].op).toBe("insert")
    expect(cached[0].op_id).toBe("compact:12345")
    expect(cached[0].char.length).toBe(50006)
  })

  it("should handle malformed server messages safely without crash", async () => {
    const client = new FlowSyncClient({
      url: "ws://localhost:8765",
      nodeId: "alice"
    })
    clients.push(client)

    const connectPromise = client.connect()
    await new Promise(r => setTimeout(r, 10))
    const wsInstance = MockWebSocket.instances[0]

    // Simulate connection AUTH_OK
    wsInstance.onmessage({ data: JSON.stringify({ type: "AUTH_OK", node_id: "alice" }) })
    await connectPromise

    let receivedError: Error | null = null
    client.onError((err) => {
      receivedError = err
    })

    // Simulate invalid JSON syntax
    wsInstance.onmessage({ data: "{invalid-json" })
    expect(receivedError).not.toBeNull()
    expect(receivedError!.message).toContain("failed to decode message")

    // Simulate JSON missing valid types
    receivedError = null
    wsInstance.onmessage({ data: JSON.stringify({ type: "UNKNOWN_TYPE" }) })
    expect(receivedError).not.toBeNull()
    expect(receivedError!.message).toContain("failed to decode message")
  })

  it("should resolve all concurrent client.connect calls to the same handshake promise", async () => {
    const client = new FlowSyncClient({
      url: "ws://localhost:8765",
      nodeId: "alice"
    })
    clients.push(client)

    // Call connect multiple times concurrently
    const p1 = client.connect()
    const p2 = client.connect()
    const p3 = client.connect()

    // They must return the identical promise instance
    expect(p1).toBe(p2)
    expect(p2).toBe(p3)

    await new Promise(r => setTimeout(r, 10))
    const wsInstance = MockWebSocket.instances[0]

    // Authenticate once
    wsInstance.onmessage({ data: JSON.stringify({ type: "AUTH_OK", node_id: "alice" }) })

    // Await all and verify they resolve together
    await Promise.all([p1, p2, p3])
    expect(client.isOnline).toBe(true)
  })

  it("should prevent prototype pollution in local queue recovery", () => {
    // Write polluted JSON payload to localStorage
    const pollutedKey = "flowsync:queue:polluter"
    const pollutedPayload = [
      {
        stream: "game:score",
        value: 100,
        timestamp: Date.now() / 1000,
        "__proto__": { "polluted": true },
        "constructor": { "prototype": { "polluted": true } }
      }
    ]
    localStorage.setItem(pollutedKey, JSON.stringify(pollutedPayload))

    const client = new FlowSyncClient({
      url: "ws://localhost:8765",
      nodeId: "polluter"
    })
    clients.push(client)

    // Recover changes via queue drain
    const recovered = client["queue"].drain()
    expect(recovered.length).toBe(1)
    
    // Assert that the parsed objects do not pollute prototypes
    const obj = recovered[0] as any
    expect(obj.hasOwnProperty("__proto__")).toBe(false)
    expect(obj.hasOwnProperty("constructor")).toBe(false)
    expect(({} as any).polluted).toBeUndefined()
  })
})
