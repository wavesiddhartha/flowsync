# Changelog

All notable changes to **FlowSync** will be documented in this file.

## [0.1.0] - 2026-06-06

### Added
- **Core backend engines**: Real-time WebSocket `FlowSyncHub` and client-side `FlowSyncNode` SDK in Python.
- **CRDT State Merging**: Full PN-Counter, 2P-Set, and Sequence Text CRDT merge algorithms resolving state conflicts automatically.
- **Offline-First Capabilities**: Offline SQLite transaction queue for python clients, syncing pending changes instantly upon reconnection.
- **JavaScript/TypeScript SDK**: Complete `flowsync-client` package with offline local storage fallbacks.
- **React UI Hooks**: Exposes standard hooks `useStream`, `useStreamValue`, and `useFlowSync` for context-aware state updates.
- **Granular Security**: PyJWT authentication and wildcard ACL check middlewares (e.g. `write:game:*`).
- **Horizontal Scaling**: Async Redis storage engine with Pub/Sub message synchronization across clustered hubs.
- **Diagnostics Dashboard**: Built-in glassmorphic HTML metrics GUI serving node counts, stream statistics, and throughput.
- **Command Line Interface**: Command-line tool `flowsync` exposing `dev` servers, `push`/`get` actions, and terminal `monitor`.
- **Integrations**: Mount helper for FastAPI apps and PostgreSQL LISTEN/NOTIFY change streams forwarder.
