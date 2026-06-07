# 🎮 FlowSync Multiplayer Bubble Arena

An interactive HTML5 Canvas multiplayer lobby syncing mouse cursors in real-time using FlowSync rooms.

## 🔍 How it Works

1. Each client tab represents a player and generates a random HSL color and unique ID.
2. The tab opens a WebSocket connection to the Hub, authenticates, and joins the `game` room (`JOIN_ROOM`).
3. When you move your mouse inside the canvas, the client broadcasts a `move` event containing its `{id, x, y, color}` coordinates to the `game` room.
4. Other tabs in the room receive the `BROADCAST` messages and render your colored bubble on their canvas in real-time.
5. Inactive players (who haven't broadcasted in >3 seconds) are automatically garbage-collected and removed from the canvas.

## 🚀 How to Run

1. Make sure the FlowSync Hub is running:
   ```bash
   flowsync dev
   ```
2. Open `index.html` in your browser.
3. Open the same file in multiple browser tabs or separate windows side-by-side to play and sync positions.
