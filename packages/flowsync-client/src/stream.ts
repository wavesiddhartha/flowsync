import { FlowSyncClient } from "./client"

export class StreamRef<T = unknown> {
  private client: FlowSyncClient
  private name: string
  private _value: T | undefined
  private listeners: Set<(v: T) => void> = new Set()
  private unsubscribes: (() => void)[] = []

  constructor(client: FlowSyncClient, name: string, initial?: T) {
    this.client = client
    this.name = name
    this._value = initial

    // Auto-subscribe to updates
    const unsub = client.on(name, (v) => {
      this._value = v as T
      this.listeners.forEach((fn) => fn(v as T))
    })
    this.unsubscribes.push(unsub)
  }

  get value(): T | undefined {
    return this._value
  }

  async push(value: T): Promise<void> {
    await this.client.push(this.name, value)
  }

  subscribe(callback: (value: T) => void): () => void {
    this.listeners.add(callback)
    if (this._value !== undefined) {
      callback(this._value)
    }
    return () => {
      this.listeners.delete(callback)
    }
  }

  async refresh(): Promise<T> {
    const val = (await this.client.get(this.name)) as T
    this._value = val
    return val
  }

  destroy(): void {
    this.unsubscribes.forEach((unsub) => unsub())
    this.unsubscribes = []
    this.listeners.clear()
  }
}

export function stream<T>(
  client: FlowSyncClient,
  name: string,
  initial?: T
): StreamRef<T> {
  return new StreamRef<T>(client, name, initial)
}
