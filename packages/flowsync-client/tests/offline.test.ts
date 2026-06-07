import { describe, it, expect, beforeEach, vi } from "vitest"
import { OfflineQueue } from "../src/offline"

// Simple mock for localStorage
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

// Stub localStorage globally
vi.stubGlobal("localStorage", mockLocalStorage)

describe("OfflineQueue Tests", () => {
  beforeEach(() => {
    mockLocalStorage.clear()
    mockLocalStorage.getItem.mockClear()
    mockLocalStorage.setItem.mockClear()
    mockLocalStorage.removeItem.mockClear()
  })

  it("should enqueue and drain changes correctly", () => {
    const queue = new OfflineQueue("alice")
    queue.enqueue("game:score", 42)
    queue.enqueue("game:score", 43)

    expect(queue.count).toBe(2)

    const changes = queue.drain()
    expect(changes.length).toBe(2)
    expect(changes[0].stream).toBe("game:score")
    expect(changes[0].value).toBe(42)
    expect(changes[1].value).toBe(43)
  })

  it("should clear the queue cleanly", () => {
    const queue = new OfflineQueue("alice")
    queue.enqueue("game:score", 100)
    expect(queue.count).toBe(1)

    queue.clear()
    expect(queue.count).toBe(0)
  })

  it("should survive browser reload via localStorage", () => {
    const queue1 = new OfflineQueue("bob")
    queue1.enqueue("doc:text", { op: "insert", char: "a" })
    expect(queue1.count).toBe(1)

    // Recreate a new OfflineQueue instance representing reload
    const queue2 = new OfflineQueue("bob")
    expect(queue2.count).toBe(1)
    
    const drained = queue2.drain()
    expect(drained.length).toBe(1)
    expect((drained[0].value as any).char).toBe("a")
  })

  it("should return items sorted by timestamp", async () => {
    const queue = new OfflineQueue("alice")
    
    // Enqueue items manually with varying timestamps to verify ordering
    // Since enqueue uses Date.now(), we mock the timestamp
    const change1 = { stream: "s1", value: "first", timestamp: 1000 }
    const change2 = { stream: "s1", value: "third", timestamp: 3000 }
    const change3 = { stream: "s1", value: "second", timestamp: 2000 }

    // Save manually to localStorage directly to test sorting
    localStorage.setItem("flowsync:queue:alice", JSON.stringify([change1, change2, change3]))

    const drained = queue.drain()
    expect(drained.length).toBe(3)
    expect(drained[0].value).toBe("first")
    expect(drained[1].value).toBe("second")
    expect(drained[2].value).toBe("third")
  })
})
