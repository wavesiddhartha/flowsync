# FlowSync Examples Gallery

This directory contains six fully functional example projects demonstrating different features of the **FlowSync** real-time, offline-first synchronization engine.

---

## 📋 Examples Overview

Here is a map of the examples and their respective synchronization and conflict resolution strategies:

| Folder | Example Name | Platform | Merge Strategy | Key Demonstration |
| :--- | :--- | :---: | :---: | :--- |
| **`react-demo/`** | [Collaborative Workspace](react-demo/) | Browser (React) | `crdt-counter`, `crdt-set`, `crdt-text` | Dynamic collaborative checklist, text editor, and offline simulation. |
| **`01-chat-app/`** | [Terminal Chat Client](01-chat-app/) | Terminal (Python) | `append` | Multi-user chat history preservation using list appending. |
| **`02-collaborative-editor/`** | [Sequence Text Editor Simulator](02-collaborative-editor/) | Terminal (Python) | `crdt-text` | Alice and Bob making concurrent offline insertions resolved via sequence CRDTs. |
| **`03-live-dashboard/`** | [Live Metrics Dashboard](03-live-dashboard/) | Browser (HTML5/Chart.js) | `lww` | Real-time streaming charts fed by a remote background process. |
| **`04-multiplayer-game/`** | [Multiplayer Bubble Arena](04-multiplayer-game/) | Browser (Canvas) | `broadcast` | Low-latency room message broadcasting for real-time mouse coordinate sync. |
| **`05-iot-sensor/`** | [Simulated IoT Resource reporter](05-iot-sensor/) | Terminal (Python) | `lww` | Continuous telemetry reporting node supplying data to the live dashboard. |

---

## 🚀 Running the Examples

Ensure you have your central Hub server running before starting any of the clients.

### 0. Start the Local Hub Server
You can launch the default server directly using the FlowSync CLI:
```bash
flowsync dev
```
*(This starts a websocket listener on `ws://127.0.0.1:8765` and pre-registers test streams).*

---

### ⚛️ Example 1: Collaborative React Workspace (`react-demo`)
A vibrant, glassmorphic React workspace demonstrating counter increments, set memberships (guest list), and document typing.

```bash
cd examples/react-demo
npm install
npm run dev
```
* **Walkthrough**: Open the browser link in two separate windows. Click **Go Offline** in one, make edits, and toggle **Go Online** to see them synchronize instantly.

---

### 💬 Example 2: Terminal Chat Client (`01-chat-app`)
A command-line chat showing list-append synchronization across multiple active terminals.

```bash
python examples/01-chat-app/run.py
```
* **Walkthrough**: Open multiple terminal windows and run the script. Enter different usernames and chat concurrently in real-time.

---

### 📝 Example 3: Sequence Editor Simulator (`02-collaborative-editor`)
A scripted simulation of Alice and Bob editing a shared document. Bob goes offline, both type concurrently, and their changes merge seamlessly upon Bob's reconnection.

```bash
python examples/02-collaborative-editor/run.py
```

---

### 📊 Example 4 & 5: Live Metrics Dashboard & CPU Sensor (`03-live-dashboard` & `05-iot-sensor`)
A real-time line chart rendering resource telemetry stream data.

1. Open `examples/03-live-dashboard/index.html` in your browser.
2. Start the telemetry reporting script:
   ```bash
   python examples/05-iot-sensor/sensor.py
   ```
3. Watch the chart update immediately in real-time as CPU usage metrics flow from the sensor script to the browser.

---

### 🎮 Example 6: Multiplayer Bubble Arena (`04-multiplayer-game`)
An interactive canvas arena syncing mouse locations.

1. Open `examples/04-multiplayer-game/index.html` in multiple browser tabs side-by-side.
2. Move your cursor around the canvas inside one tab, and watch your bubble move instantly in all other tabs.
