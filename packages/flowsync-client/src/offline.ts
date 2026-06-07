import { PendingChange } from "./protocol"

export class OfflineQueue {
  private static readonly MAX_QUEUE_SIZE = 10_000
  private key: string
  private useMemory: boolean
  private memoryStore: PendingChange[] = []

  constructor(nodeId: string) {
    this.key = `flowsync:queue:${nodeId}`
    this.useMemory = typeof localStorage === "undefined"
  }

  enqueue(stream: string, value: unknown): void {
    const change: PendingChange = {
      stream,
      value,
      timestamp: Date.now() / 1000
    }
    const all = this.drain_sync()
    if (all.length >= OfflineQueue.MAX_QUEUE_SIZE) {
      all.shift() // Evict oldest to prevent localStorage QuotaExceededError
    }
    all.push(change)
    this.save(all)
  }

  drain(): PendingChange[] {
    // Returns all pending changes, sorted by timestamp
    return this.drain_sync().sort((a, b) => a.timestamp - b.timestamp)
  }

  clear(): void {
    if (this.useMemory) {
      this.memoryStore = []
    } else {
      try {
        localStorage.removeItem(this.key)
      } catch {
        this.memoryStore = []
      }
    }
  }

  removeProgress(count: number): void {
    if (count <= 0) return
    const all = this.drain_sync()
    all.splice(0, count)
    this.save(all)
  }

  get count(): number {
    return this.drain_sync().length
  }

  private drain_sync(): PendingChange[] {
    if (this.useMemory) {
      return [...this.memoryStore]
    }
    try {
      const data = localStorage.getItem(this.key)
      if (!data) return []
      const parsed = JSON.parse(data)
      if (!Array.isArray(parsed)) return []
      
      // Sanitize parsed objects from localStorage to prevent poisoning / prototype pollution
      const sanitize = (obj: any): void => {
        if (obj && typeof obj === "object") {
          if ("__proto__" in obj) delete obj.__proto__
          if ("constructor" in obj) delete obj.constructor
          if ("prototype" in obj) delete obj.prototype
          for (const key in obj) {
            if (Object.prototype.hasOwnProperty.call(obj, key)) {
              sanitize(obj[key])
            }
          }
        }
      }
      
      sanitize(parsed)
      return parsed as PendingChange[]
    } catch {
      // Recover existing items from memory if localStorage fails or is corrupted
      return [...this.memoryStore]
    }
  }

  private save(changes: PendingChange[]): void {
    // Set memoryStore as a fallback / backup
    this.memoryStore = changes
    if (!this.useMemory) {
      try {
        localStorage.setItem(this.key, JSON.stringify(changes))
      } catch {
        // Fallback to memory on localStorage error (quota exceeded, etc.)
        this.useMemory = true
      }
    }
  }
}
