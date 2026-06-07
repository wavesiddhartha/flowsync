# flowsync-client

> **TypeScript/JavaScript Client SDK for FlowSync — the universal real-time, offline-first state synchronization engine.**

FlowSync-Client handles client-side caching, reconnect handshakes, React context providers, and offline transaction queueing for browsers and Node.js environments.

---

## 📦 Installation

```bash
npm install flowsync-client
```

---

## 🚀 Quick Start (Vanilla JavaScript/TypeScript)

```typescript
import { FlowSyncClient } from 'flowsync-client'

// 1. Initialize client
const client = new FlowSyncClient({
  url: 'ws://localhost:8765',
  authToken: 'user-jwt-token',
  offlineMode: True,
  reconnect: True
})

// 2. Connect
await client.connect()

// 3. Subscribe to a stream
client.on('chat:messages', (value, meta) => {
  console.log('Received updated list of messages:', value)
})

// 4. Push updates
await client.push('chat:messages', {
  user: 'alice',
  text: 'Hello from client SDK!'
})
```

---

## ⚛️ React Integration

### 1. Wrap App in Provider
```tsx
import React from 'react'
import { FlowSyncClient, FlowSyncProvider } from 'flowsync-client'
import Workspace from './Workspace'

const client = new FlowSyncClient({ url: 'ws://localhost:8765' })

export default function App() {
  return (
    <FlowSyncProvider client={client}>
      <Workspace />
    </FlowSyncProvider>
  )
}
```

### 2. Synchronize Stream values in Components
```tsx
import React from 'react'
import { useStream, useFlowSync } from 'flowsync-client'

export default function Workspace() {
  const { isOnline, pendingCount } = useFlowSync()
  const [docText, pushEdit] = useStream<string>('doc:shared', '')

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    pushEdit({
      op: 'insert',
      pos: e.target.selectionStart - 1,
      char: e.target.value[e.target.selectionStart - 1]
    } as any)
  }

  return (
    <div>
      <span className={isOnline ? 'online' : 'offline'}>
        {isOnline ? 'Connected' : 'Offline'}
      </span>
      <textarea value={docText} onChange={handleChange} />
    </div>
  )
}
```

---

## 🛡️ Features
* **Conflict-Free Replicated Data Types (CRDTs)**: Merges counters, sets, and sequence texts automatically.
* **Offline-First Storage**: Saves pending pushes to `localStorage` when offline and replays them automatically upon reconnection.
* **Handshake Recovery**: Synchronizes missed server updates during reconnection.
* **TypeScript Types**: Fully typed API.
* **SSR Safe**: Safe for Next.js / server-side rendering.

---

## 📄 License
MIT
