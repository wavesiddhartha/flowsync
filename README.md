<p align="center">
  <img src="assets/logo.png" alt="FlowSync Logo" width="180"/>
</p>

<h1 align="center">FlowSync</h1>

<p align="center">
  <strong>One library to rule your state — online, offline, edge, everywhere.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License"/>
  <img src="https://img.shields.io/badge/Python-3.10%2B-blue" alt="Python Version"/>
  <img src="https://img.shields.io/badge/TypeScript-Client-cyan" alt="TypeScript"/>
  <img src="https://img.shields.io/badge/build-passing-success" alt="Build Status"/>
</p>

---

## 📖 Introduction

**FlowSync** is a production-grade, open-source, real-time collaborative state synchronization engine. It bridges the gap between client applications, backend services, and edge devices, combining Socket.io-style messaging namespaces with Conflict-Free Replicated Data Types (CRDTs) and local offline databases.

Whether you are building a collaborative document editor, a real-time multiplayer game, an IoT sensor telemetry dashboard, or a multi-user workspace, FlowSync automatically guarantees that all your client states converge seamlessly—even after long network disruptions.

---

## ❓ The Problem: Why FlowSync?

Building collaborative, real-time applications is notoriously difficult. Developers constantly run into three major challenges:

1. **State Divergence & Overwrite Conflicts**: When multiple users edit the same piece of data (like a checklist or text document) at the same time, standard "Last-Write-Wins" (LWW) databases cause the last person's network latency to win, silently overwriting and losing other users' changes.
2. **Connection Flakiness (The Offline Gap)**: Users on mobile devices or unstable networks drop connections constantly. Queuing local changes and replaying them on reconnect in the *exact* logical order—while dealing with changes other users made in the meantime—usually requires thousands of lines of fragile synchronization code.
3. **Horizontal Scaling Complexities**: Real-time WebSockets keep connections open. When you scale your backend horizontally to handle more traffic, users get connected to different server instances. Coordinating changes across load-balanced instances in real-time is a complex networking headache.
4. **WebSocket Security & Rate Limiting**: Securing raw WebSocket connections, applying granular authentication, restricting channel access (ACLs), and protecting servers against message-flooding Denial of Service (DoS) requires custom middleware.

---

## ⚡ The Solution: What FlowSync Solves

FlowSync resolves these issues out of the box with a zero-dependency-on-client library architecture:

* **Mathematical State Convergence (CRDTs)**: Supports collaborative counters (**PN-Counters**), guest lists/inventories (**2P-Sets**), and concurrent rich-text lines (**Sequence Text CRDTs**) that merge automatically without locks or central coordinators.
* **Persistent Offline Queueing**: When offline, changes are persisted to a local SQLite database (on Python nodes) or `localStorage` (in browsers). Once connectivity returns, a specialized `RECONNECT` handshake replays changes and fetches missed updates.
* **Pipelined Redis Clustering**: Clustered Hub deployments sync instantly over a Redis Pub/Sub backend with auto-reconnect listeners.
* **Granular Production Security**:
  * **JWT Authentication**: Secure handshake token validation.
  * **Wildcard ACL Rules**: Limit stream actions with rules like `write:game:lobby` or `read:chat:*`.
  * **Token Bucket Rate Limiting**: Enforce global, per-node, and per-stream rate limits.
  * **GDPR Logging & Size Protections**: Log IP masking (`127.0.***.***`), max payload caps (10MB), and automated operation compaction to prevent memory bloat.

---

## 📦 Project Structure

FlowSync is built as a monorepo containing:

```bash
├── packages/
│   ├── flowsync-core/       # Python SDK & Hub (PyPI: flowsync-core)
│   └── flowsync-client/     # TypeScript/JavaScript Client (NPM: flowsync-client)
├── assets/
│   └── logo.png             # FlowSync Logo
├── examples/
│   ├── react-demo/          # Vite + React collaborative demo (Vibrant Glassmorphic UI)
│   ├── 01-chat-app/         # Terminal-based chat using append-log
│   ├── 02-collaborative-editor/ # Text CRDT concurrent editing simulation
│   ├── 03-live-dashboard/   # Real-time metrics visualization with Chart.js
│   ├── 04-multiplayer-game/ # HTML5 canvas multiplayer dot game
│   └── 05-iot-sensor/       # Simulating sensor metrics reporting
```

---

## 🚀 Quick Start - Python Backend & Node Client

### 1. Install the Core Package
```bash
pip install flowsync-core
```

### 2. Spawn the Central Hub Server
Start a Hub and register streams with specific merge rules:

```python
import asyncio
from flowsync import FlowSyncHub

async def main():
    # Start the Hub on port 8765
    hub = FlowSyncHub(host="127.0.0.1", port=8765)
    
    # Register streams with different CRDT merge behaviors
    hub.stream("counter:value", merge_rule="crdt-counter", initial={"increments": {}, "decrements": {}})
    hub.stream("text:doc", merge_rule="crdt-text", initial=[])
    
    print("FlowSync Hub starting on ws://127.0.0.1:8765...")
    await hub.start()

if __name__ == "__main__":
    asyncio.run(main())
```

### 3. Connect a Python Node Client
```python
import asyncio
from flowsync import FlowSyncNode

async def main():
    node = FlowSyncNode("ws://127.0.0.1:8765", node_id="sensor-node-1")
    await node.connect()
    print("Connected to FlowSync Hub!")

    # Push a CRDT counter increment operation
    await node.push("counter:value", {"op": "increment", "amount": 5})

    # Read the current mathematically merged value
    current_count = await node.get("counter:value")
    print(f"Current Sync Count: {current_count}")

    await node.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
```

---

## ⚛️ Quick Start - React Client SDK

### 1. Install Client Library
```bash
npm install flowsync-client
```

### 2. Wrap App with `FlowSyncProvider`
```tsx
import React from 'react'
import { FlowSyncClient, FlowSyncProvider } from 'flowsync-client'
import Workspace from './Workspace'

const client = new FlowSyncClient({
  url: 'ws://localhost:8765',
  authToken: 'user-jwt-token'
})

export default function App() {
  return (
    <FlowSyncProvider client={client}>
      <Workspace />
    </FlowSyncProvider>
  )
}
```

### 3. Bind Streams to React Components
Use hooks to synchronize values, execute pushes, and track network/offline status:

```tsx
import React from 'react'
import { useStream, useFlowSync } from 'flowsync-client'

export default function Workspace() {
  const { isOnline, pendingCount } = useFlowSync()
  
  // Instantly binds state 'doc:text' as a sequence-text CRDT
  const [documentText, pushEdit] = useStream<string>('doc:text', '')

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    // Push insert operation at cursor position
    pushEdit({
      op: 'insert',
      pos: e.target.selectionStart - 1,
      char: e.target.value[e.target.selectionStart - 1]
    } as any)
  }

  return (
    <div className="workspace-container">
      <header>
        <span className={`badge ${isOnline ? 'online' : 'offline'}`}>
          {isOnline ? 'Online' : 'Offline'}
        </span>
        {pendingCount > 0 && (
          <span className="warning">{pendingCount} local changes queued</span>
        )}
      </header>

      <textarea value={documentText} onChange={handleChange} />
    </div>
  )
}
```

---

## ⚡ Interactive Local Walkthrough (Vite + React)

We've packaged a stunning, glassmorphic diagnostics dashboard and live whiteboard showcase to test concurrent edits and offline recovery.

1. **Run the local Hub Server**:
   ```bash
   flowsync dev
   ```
2. **Launch the React App**:
   ```bash
   cd examples/react-demo
   npm install
   npm run dev
   ```
3. **Open multiple tabs** side-by-side:
   * Toggle the **Offline** simulation button on one client.
   * Edit text, increment counters, or add list items.
   * Observe how the offline tab queues changes, showing a warning indicator.
   * Toggle **Online** and see both tabs converge instantly without conflict.

---

## 🛡️ Production & Security Configuration

FlowSync is built for production environments. Ensure you configure:

* **JWT Secret Keys**: Enable authentication in production by providing a secret validation callback to `FlowSyncHub(auth_fn=jwt_auth("your-secret-key"))`.
* **Clustering**: To scale to multiple nodes, spin up a Redis instance and supply the URL to the Hub: `FlowSyncHub(redis_url="redis://localhost:6379")`.
* **Diagnostics Panel**: Mount FlowSync directly under a FastAPI/Starlette server and access the live monitoring suite at `/dashboard` with query token protections.

---

## 📄 License
FlowSync is open-source software licensed under the [MIT License](LICENSE).
