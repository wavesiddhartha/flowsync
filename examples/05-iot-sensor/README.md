# 🔌 FlowSync Simulated IoT CPU Sensor

A python telemetry script that pushes simulated resource metrics to the FlowSync Hub.

## 🔍 How it Works

1. The script connects to the Hub at `ws://localhost:8765` as node ID `iot-sensor-cpu`.
2. It runs a loop every 0.5 seconds, generating simulated CPU load values (combining a sine-wave baseline, random spikes, and high-frequency noise).
3. It pushes the cpu metrics directly to the `metrics:cpu` stream.
4. If the Hub goes down, the script handles reconnection logic in the background.

## 🚀 How to Run

1. Make sure the FlowSync Hub is running:
   ```bash
   flowsync dev
   ```
2. Run the sensor script:
   ```bash
   python sensor.py
   ```
3. To view the metrics in real-time, open the [Live Metrics Dashboard](../03-live-dashboard/index.html) in your browser.
