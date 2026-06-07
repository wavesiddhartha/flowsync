# 📊 FlowSync Live Metrics Dashboard

A web-based diagnostics line chart showing live CPU telemetry stream data using Chart.js.

## 🔍 How it Works

1. The page opens a standard WebSocket connection to the FlowSync Hub at `ws://localhost:8765`.
2. It sends an `AUTH` message and then subscribes to the `metrics:cpu` stream.
3. Every time a new telemetry value is pushed to `metrics:cpu`, the page receives an `UPDATE` message and pushes it into the Chart.js line dataset.
4. The chart updates dynamically in real-time, displaying a rolling window of the last 30 data points.

## 🚀 How to Run

1. Make sure the FlowSync Hub is running:
   ```bash
   flowsync dev
   ```
2. Open `index.html` in your browser.
3. To stream data to the dashboard, run the CPU sensor script in a separate terminal:
   ```bash
   python ../05-iot-sensor/sensor.py
   ```
