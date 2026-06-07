# 📝 FlowSync Collaborative Text Editor Simulator

A scripted simulation demonstrating how FlowSync's Sequence Text CRDT merges concurrent insertions without a central lock.

## 🔍 How it Works

1. The script automatically launches a FlowSync Hub in the background.
2. It instantiates two distinct client nodes: **Alice** and **Bob**, subscribing them both to the `doc:shared` stream.
3. Alice pushes the initial state `"FlowSync"`.
4. The script simulates Bob losing internet connectivity (`bob.disconnect()`).
5. **Alice (Online)**: Inserts `" is fast"` at position 8 -> document becomes `"FlowSync is fast"`.
6. **Bob (Offline)**: Concurrently inserts `"Awesome "` at position 0. Because Bob is offline, the operation is queued locally in Bob's SQLite database. Bob's local text becomes `"Awesome FlowSync"`.
7. Bob reconnects (`bob.connect()`).
8. FlowSync automatically drains Bob's SQLite queue, uploads the pending operations to the Hub, reconciles character indices using Sequence CRDT transformation rules, and broadcasts the converged result: **`"Awesome FlowSync is fast"`** to both users.

## 🚀 How to Run

Simply run the script. No other servers are required:
```bash
python run.py
```
