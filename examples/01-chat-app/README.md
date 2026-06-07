# 💬 FlowSync Terminal Chat Client

A lightweight CLI chat client demonstrating the **append** merge rule of FlowSync.

## 🔍 How it Works

1. The client prompts you for a username.
2. It connects to the FlowSync Hub at `ws://localhost:8765` and subscribes to the stream `chat:messages`.
3. When you type a message, it pushes it as a dictionary `{"user": username, "text": user_input}` to the `chat:messages` stream.
4. The server merges the update using the `append` rule, appending it to the history list of messages.
5. All connected chat clients instantly receive the updated list and redraw the terminal console showing the last 15 messages.

## 🚀 How to Run

1. Make sure the FlowSync Hub is running:
   ```bash
   flowsync dev
   ```
2. Run the chat client:
   ```bash
   python run.py
   ```
3. Open a second terminal window and run another instance of the client with a different username to chat collaboratively.
