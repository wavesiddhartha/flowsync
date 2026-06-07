import json
import os
from typing import Dict, Any, Optional
from fastapi import FastAPI, APIRouter, HTTPException, Request, Depends
from fastapi.responses import HTMLResponse

# Premium dark mode diagnostics dashboard
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FlowSync Diagnostics Hub</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&family=Plus+Jakarta+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {
            --bg-color: #080B11;
            --card-bg: rgba(17, 24, 39, 0.45);
            --border-color: rgba(255, 255, 255, 0.08);
            --text-primary: #f3f4f6;
            --text-secondary: #9ca3af;
            --accent-primary: #6366f1;
            --accent-secondary: #06b6d4;
            --success: #10b981;
            --warning: #f59e0b;
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Plus Jakarta Sans', sans-serif;
            background: radial-gradient(circle at 10% 20%, rgba(24, 30, 48, 1) 0%, rgba(8, 11, 17, 1) 90%);
            color: var(--text-primary);
            min-height: 100vh;
            padding: 2rem;
            -webkit-font-smoothing: antialiased;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2.5rem;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 1.5rem;
        }

        h1 {
            font-family: 'Outfit', sans-serif;
            font-size: 2.25rem;
            font-weight: 700;
            background: linear-gradient(135deg, #ffffff 0%, #a5b4fc 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .tagline {
            color: var(--text-secondary);
            font-size: 0.95rem;
            margin-top: 0.25rem;
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }

        .card {
            background: var(--card-bg);
            backdrop-filter: blur(24px);
            -webkit-backdrop-filter: blur(24px);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 1.5rem;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
            position: relative;
            overflow: hidden;
            transition: transform 0.2s ease, border-color 0.2s ease;
        }

        .card:hover {
            transform: translateY(-2px);
            border-color: rgba(99, 102, 241, 0.3);
        }

        .card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 1px;
            background: linear-gradient(90deg, rgba(255, 255, 255, 0) 0%, rgba(255, 255, 255, 0.15) 50%, rgba(255, 255, 255, 0) 100%);
        }

        .card-title {
            color: var(--text-secondary);
            font-size: 0.85rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.5rem;
        }

        .card-value {
            font-family: 'Outfit', sans-serif;
            font-size: 2rem;
            font-weight: 700;
            background: linear-gradient(to right, #ffffff, #a5b4fc);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .card-accent-value {
            background: linear-gradient(to right, #6366f1, #06b6d4);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .grid-layout {
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 2rem;
        }

        @media (max-width: 768px) {
            .grid-layout {
                grid-template-columns: 1fr;
            }
        }

        .panel {
            background: var(--card-bg);
            backdrop-filter: blur(24px);
            -webkit-backdrop-filter: blur(24px);
            border: 1px solid var(--border-color);
            border-radius: 20px;
            padding: 2rem;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
        }

        .panel-header {
            margin-bottom: 1.5rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .panel-title {
            font-family: 'Outfit', sans-serif;
            font-size: 1.25rem;
            font-weight: 600;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            text-align: left;
        }

        th {
            color: var(--text-secondary);
            font-size: 0.85rem;
            font-weight: 600;
            padding: 0.75rem 1rem;
            border-bottom: 1px solid var(--border-color);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        td {
            padding: 1rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.04);
            font-size: 0.95rem;
        }

        tr:last-child td {
            border-bottom: none;
        }

        .clickable-row {
            cursor: pointer;
            transition: background-color 0.2s ease, transform 0.1s ease;
        }

        .clickable-row:hover {
            background-color: rgba(99, 102, 241, 0.08);
        }

        .clickable-row:active {
            transform: scale(0.995);
        }

        .badge {
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.8rem;
            font-weight: 600;
        }

        .badge-success {
            background: rgba(16, 185, 129, 0.1);
            color: var(--success);
            border: 1px solid rgba(16, 185, 129, 0.2);
        }

        .badge-warning {
            background: rgba(245, 158, 11, 0.1);
            color: var(--warning);
            border: 1px solid rgba(245, 158, 11, 0.2);
        }

        .stream-name {
            font-family: monospace;
            color: #a5b4fc;
            font-size: 0.95rem;
        }

        .uptime-badge {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid var(--border-color);
            padding: 0.5rem 1rem;
            border-radius: 10px;
            font-size: 0.85rem;
            font-weight: 500;
        }

        /* Backdrop styles */
        .backdrop {
            position: fixed;
            top: 0;
            left: 0;
            width: 100vw;
            height: 100vh;
            background: rgba(0, 0, 0, 0.6);
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            z-index: 999;
            opacity: 0;
            pointer-events: none;
            transition: opacity 0.3s cubic-bezier(0.16, 1, 0.3, 1);
        }

        .backdrop.active {
            opacity: 1;
            pointer-events: auto;
        }

        /* Drawer styles */
        .drawer {
            position: fixed;
            top: 0;
            right: -500px;
            width: 500px;
            height: 100vh;
            background: rgba(10, 14, 23, 0.95);
            backdrop-filter: blur(30px);
            -webkit-backdrop-filter: blur(30px);
            border-left: 1px solid var(--border-color);
            z-index: 1000;
            transition: right 0.4s cubic-bezier(0.16, 1, 0.3, 1);
            display: flex;
            flex-direction: column;
            padding: 2.5rem 2rem;
            box-shadow: -15px 0 40px rgba(0, 0, 0, 0.6);
        }

        @media (max-width: 550px) {
            .drawer {
                width: 100%;
                right: -100%;
            }
        }

        .drawer.open {
            right: 0;
        }

        .drawer-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 1.25rem;
        }

        .drawer-title {
            font-family: 'Outfit', sans-serif;
            font-size: 1.5rem;
            font-weight: 700;
            background: linear-gradient(135deg, #ffffff 0%, #a5b4fc 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .close-btn {
            background: transparent;
            border: none;
            color: var(--text-secondary);
            font-size: 2.25rem;
            cursor: pointer;
            line-height: 1;
            transition: color 0.2s, transform 0.2s;
        }

        .close-btn:hover {
            color: #ef4444;
            transform: rotate(90deg);
        }

        .drawer-content {
            overflow-y: auto;
            flex-grow: 1;
            display: flex;
            flex-direction: column;
            gap: 1.75rem;
            padding-right: 0.5rem;
        }

        /* Customize scrollbars for the drawer */
        .drawer-content::-webkit-scrollbar {
            width: 6px;
        }
        
        .drawer-content::-webkit-scrollbar-track {
            background: transparent;
        }
        
        .drawer-content::-webkit-scrollbar-thumb {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 9999px;
        }
        
        .drawer-content::-webkit-scrollbar-thumb:hover {
            background: rgba(255, 255, 255, 0.25);
        }

        .drawer-section {
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }

        .drawer-label {
            font-size: 0.75rem;
            font-weight: 700;
            text-transform: uppercase;
            color: var(--text-secondary);
            letter-spacing: 0.08em;
        }

        pre {
            background: rgba(0, 0, 0, 0.45);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1.25rem;
            overflow-x: auto;
            font-family: monospace;
            font-size: 0.85rem;
            color: #e5e7eb;
        }

        code {
            white-space: pre-wrap;
            word-break: break-all;
        }

        .history-list {
            display: flex;
            flex-direction: column;
            gap: 0.75rem;
        }

        .history-item {
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid var(--border-color);
            border-radius: 10px;
            padding: 1rem;
            font-size: 0.85rem;
            transition: border-color 0.2s;
        }

        .history-item:hover {
            border-color: rgba(99, 102, 241, 0.25);
        }

        .history-header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 0.5rem;
        }

        .history-node {
            font-weight: 600;
            color: #a5b4fc;
            font-family: monospace;
        }

        .history-time {
            color: var(--text-secondary);
            font-size: 0.75rem;
        }

        .history-value {
            font-family: monospace;
            font-size: 0.8rem;
            color: #9ca3af;
            word-break: break-all;
            background: rgba(0, 0, 0, 0.2);
            padding: 0.5rem;
            border-radius: 6px;
            margin-top: 0.25rem;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div>
                <h1>FlowSync Diagnostics</h1>
                <p class="tagline">Live hub health & stream synchronization metrics</p>
            </div>
            <div id="uptime" class="uptime-badge">Uptime: --:--:--</div>
        </header>

        <!-- Live Throughput Line Graph -->
        <section class="panel" style="margin-bottom: 2rem;">
            <div class="panel-header">
                <div class="panel-title">Throughput Activity (Live msg/s)</div>
            </div>
            <div style="height: 180px; width: 100%; position: relative;">
                <canvas id="throughputChart"></canvas>
            </div>
        </section>

        <section class="stats-grid">
            <div class="card">
                <div class="card-title">Active Connections</div>
                <div id="stat-nodes" class="card-value">0</div>
            </div>
            <div class="card">
                <div class="card-title">Throughput</div>
                <div id="stat-throughput" class="card-value card-accent-value">0 msg/s</div>
            </div>
            <div class="card">
                <div class="card-title">Total Messages</div>
                <div id="stat-total-msg" class="card-value">0</div>
            </div>
            <div class="card">
                <div class="card-title">Conflicts Resolved</div>
                <div id="stat-conflicts" class="card-value card-accent-value">0</div>
            </div>
        </section>

        <section class="grid-layout">
            <div class="panel">
                <div class="panel-header">
                    <div class="panel-title">Active Data Streams</div>
                    <div id="stream-count" class="badge badge-success">0 Streams</div>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Stream Name</th>
                            <th>Subscribers</th>
                            <th>Last Sync</th>
                        </tr>
                    </thead>
                    <tbody id="streams-table-body">
                        <tr>
                            <td colspan="3" style="text-align: center; color: var(--text-secondary);">No streams active</td>
                        </tr>
                    </tbody>
                </table>
            </div>

            <div class="panel">
                <div class="panel-header">
                    <div class="panel-title">Hub Overview</div>
                </div>
                <div style="display: flex; flex-direction: column; gap: 1rem; font-size: 0.9rem;">
                    <div style="display: flex; justify-content: space-between; border-bottom: 1px solid rgba(255, 255, 255, 0.05); padding-bottom: 0.5rem;">
                        <span style="color: var(--text-secondary);">Server Mode</span>
                        <span style="font-weight: 600; color: var(--success);">Clustered (Redis)</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; border-bottom: 1px solid rgba(255, 255, 255, 0.05); padding-bottom: 0.5rem;">
                        <span style="color: var(--text-secondary);">Transport</span>
                        <span style="font-family: monospace;">WebSockets</span>
                    </div>
                    <div style="display: flex; justify-content: space-between;">
                        <span style="color: var(--text-secondary);">Status</span>
                        <span class="badge badge-success" style="padding: 0.15rem 0.5rem;">Healthy</span>
                    </div>
                </div>
            </div>
        </section>
    </div>

    <!-- Backdrop Overlay -->
    <div id="backdrop" class="backdrop" onclick="closeInspector()"></div>

    <!-- Side Drawer Inspector -->
    <div id="inspector-drawer" class="drawer">
        <div class="drawer-header">
            <div class="drawer-title" id="inspector-stream-name">Stream Details</div>
            <button class="close-btn" onclick="closeInspector()">&times;</button>
        </div>
        <div class="drawer-content">
            <div class="drawer-section">
                <div class="drawer-label">Merge Strategy</div>
                <span class="badge badge-warning" id="inspector-merge-rule">-</span>
            </div>
            <div class="drawer-section">
                <div class="drawer-label">Active Subscribers</div>
                <div id="inspector-subscribers" style="font-weight: 600;">0</div>
            </div>
            <div class="drawer-section">
                <div class="drawer-label">Computed Value</div>
                <pre><code id="inspector-computed-value">{}</code></pre>
            </div>
            <div class="drawer-section">
                <div class="drawer-label">Raw CRDT Structure</div>
                <pre><code id="inspector-raw-value">{}</code></pre>
            </div>
            <div class="drawer-section">
                <div class="drawer-label">Operation Sync History (Last 20)</div>
                <div class="history-list" id="inspector-history">
                    <div style="color: var(--text-secondary);">Select a stream to load history.</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        function formatUptime(secs) {
            const h = Math.floor(secs / 3600).toString().padStart(2, '0');
            const m = Math.floor((secs % 3600) / 60).toString().padStart(2, '0');
            const s = (secs % 60).toString().padStart(2, '0');
            return `Uptime: ${h}:${m}:${s}`;
        }
        function escapeHtml(str) {
            if (!str) return '';
            const div = document.createElement('div');
            div.textContent = str;
            return div.innerHTML.replace(/'/g, '&#39;').replace(/"/g, '&quot;');
        }

        // Throughput Chart Logic
        let throughputChart = null;
        const maxDataPoints = 30;
        const chartLabels = Array(maxDataPoints).fill('');
        const chartData = Array(maxDataPoints).fill(0);

        function initChart() {
            const canvas = document.getElementById('throughputChart');
            if (typeof Chart === 'undefined') {
                console.warn("Chart.js not loaded. Throughput line chart disabled.");
                canvas.outerHTML = `
                    <div style="height: 100%; display: flex; align-items: center; justify-content: center; color: var(--text-secondary); font-size: 0.9rem; border: 1px dashed var(--border-color); border-radius: 12px; background: rgba(255,255,255,0.01);">
                        Interactive line chart unavailable (Offline CDN)
                    </div>
                `;
                return;
            }
            const ctx = canvas.getContext('2d');
            const gradient = ctx.createLinearGradient(0, 0, 0, 180);
            gradient.addColorStop(0, 'rgba(99, 102, 241, 0.35)');
            gradient.addColorStop(1, 'rgba(99, 102, 241, 0.0)');

            throughputChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: chartLabels,
                    datasets: [{
                        label: 'Throughput',
                        data: chartData,
                        borderColor: '#6366f1',
                        borderWidth: 2,
                        pointRadius: 0,
                        pointHoverRadius: 4,
                        backgroundColor: gradient,
                        fill: true,
                        tension: 0.4
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            mode: 'index',
                            intersect: false,
                            backgroundColor: 'rgba(17, 24, 39, 0.95)',
                            titleColor: '#f3f4f6',
                            bodyColor: '#f3f4f6',
                            borderColor: 'rgba(255, 255, 255, 0.08)',
                            borderWidth: 1
                        }
                    },
                    scales: {
                        x: {
                            display: false,
                            grid: { display: false }
                        },
                        y: {
                            min: 0,
                            suggestedMax: 10,
                            grid: {
                                color: 'rgba(255, 255, 255, 0.04)',
                                drawBorder: false
                            },
                            ticks: {
                                color: '#9ca3af',
                                font: { size: 10, family: 'Plus Jakarta Sans' }
                            }
                        }
                    }
                }
            });
        }

        function updateChart(newValue) {
            if (!throughputChart) return;
            chartData.push(newValue);
            chartData.shift();
            throughputChart.update();
        }

        // Stream Inspector Logic
        async function inspectStream(streamName) {
            document.getElementById('backdrop').classList.add('active');
            document.getElementById('inspector-drawer').classList.add('open');
            
            document.getElementById('inspector-stream-name').textContent = streamName;
            document.getElementById('inspector-merge-rule').textContent = 'Loading...';
            document.getElementById('inspector-subscribers').textContent = '--';
            document.getElementById('inspector-computed-value').textContent = 'Loading...';
            document.getElementById('inspector-raw-value').textContent = 'Loading...';
            document.getElementById('inspector-history').innerHTML = '<div style="color: var(--text-secondary);">Loading history...</div>';
            
            try {
                const basePath = window.location.pathname.endsWith('/') ? window.location.pathname : window.location.pathname + '/';
                const params = new URLSearchParams(window.location.search);
                const token = params.get('token') || '';
                const tokenQS = token ? `?token=${encodeURIComponent(token)}` : '';
                
                const res = await fetch(basePath + 'api/streams/' + encodeURIComponent(streamName) + tokenQS);
                if (!res.ok) throw new Error("Failed to fetch stream details");
                const data = await res.json();
                
                document.getElementById('inspector-merge-rule').textContent = data.merge_rule;
                document.getElementById('inspector-subscribers').textContent = data.subscribers;
                
                document.getElementById('inspector-computed-value').textContent = JSON.stringify(data.computed_value, null, 2);
                document.getElementById('inspector-raw-value').textContent = JSON.stringify(data.raw_value, null, 2);
                
                const historyContainer = document.getElementById('inspector-history');
                if (data.history.length === 0) {
                    historyContainer.innerHTML = '<div style="color: var(--text-secondary);">No history recorded yet</div>';
                } else {
                    historyContainer.innerHTML = data.history.map(item => {
                        const date = new Date(item.ts * 1000).toLocaleTimeString();
                        return `
                            <div class="history-item">
                                <div class="history-header">
                                    <span class="history-node">${escapeHtml(item.node_id)}</span>
                                    <span class="history-time">${escapeHtml(date)}</span>
                                </div>
                                <div class="history-value">Raw state: ${escapeHtml(JSON.stringify(item.value))}</div>
                                ${item.metadata && Object.keys(item.metadata).length > 0 ? `<div class="history-value" style="font-size:0.75rem; color:#6b7280; background:transparent; padding:0;">Meta: ${escapeHtml(JSON.stringify(item.metadata))}</div>` : ''}
                            </div>
                        `;
                    }).join('');
                }
            } catch (err) {
                console.error("Inspector error:", err);
                document.getElementById('inspector-merge-rule').textContent = 'Error';
                document.getElementById('inspector-computed-value').textContent = 'Failed to load details.';
                document.getElementById('inspector-raw-value').textContent = 'Failed to load details.';
                document.getElementById('inspector-history').innerHTML = '<div style="color: #ef4444;">Could not load history.</div>';
            }
        }

        function closeInspector() {
            document.getElementById('backdrop').classList.remove('active');
            document.getElementById('inspector-drawer').classList.remove('open');
        }

        async function fetchStats() {
            try {
                const basePath = window.location.pathname.endsWith('/') ? window.location.pathname : window.location.pathname + '/';
                const params = new URLSearchParams(window.location.search);
                const token = params.get('token') || '';
                const tokenQS = token ? `?token=${encodeURIComponent(token)}` : '';

                const res = await fetch(basePath + 'api/stats' + tokenQS);
                if (!res.ok) throw new Error("API error");
                const data = await res.json();
                
                document.getElementById('stat-nodes').textContent = data.nodes;
                document.getElementById('stat-throughput').textContent = `${data.messages_per_second} msg/s`;
                document.getElementById('stat-total-msg').textContent = data.total_messages.toLocaleString();
                document.getElementById('stat-conflicts').textContent = data.conflicts_resolved;
                document.getElementById('uptime').textContent = formatUptime(data.uptime_seconds);
                document.getElementById('stream-count').textContent = `${data.streams_count} Streams`;
                
                updateChart(data.messages_per_second);

                const tbody = document.getElementById('streams-table-body');
                if (data.streams.length === 0) {
                    tbody.innerHTML = `<tr><td colspan="3" style="text-align: center; color: var(--text-secondary);">No streams active</td></tr>`;
                } else {
                    tbody.innerHTML = data.streams.map(s => `
                        <tr class="clickable-row" onclick="inspectStream('${escapeHtml(s.name)}')">
                            <td><span class="stream-name">${escapeHtml(s.name)}</span></td>
                            <td>${escapeHtml(String(s.subscribers))}</td>
                            <td>${escapeHtml(s.last_updated)}</td>
                        </tr>
                    `).join('');
                }
            } catch (err) {
                console.error("Error updating diagnostics stats:", err);
            }
        }

        initChart();
        setInterval(fetchStats, 1000);
        fetchStats();
    </script>
</body>
</html>
"""
def create_dashboard_router(hub, admin_token: Optional[str] = None) -> APIRouter:
    """
    Creates an APIRouter for serving the diagnostics dashboard
    and its JSON statistics endpoint.
    """
    router = APIRouter()
    token = admin_token or os.environ.get("FLOWSYNC_DASHBOARD_TOKEN")

    async def check_token(request: Request):
        if not token:
            return
        
        # Check query parameter first (makes it easier for browsers)
        req_token = request.query_params.get("token")
        
        # Fallback to Authorization header
        if not req_token:
            auth_header = request.headers.get("Authorization")
            if auth_header and auth_header.startswith("Bearer "):
                req_token = auth_header.split(" ", 1)[1]
                
        if req_token != token:
            raise HTTPException(status_code=401, detail="Unauthorized dashboard access")

    @router.get("/", response_class=HTMLResponse)
    async def get_dashboard(request: Request, _ = Depends(check_token)):
        return DASHBOARD_HTML

    @router.get("/api/stats")
    async def get_stats(request: Request, _ = Depends(check_token)):
        return hub.stats()

    @router.get("/api/streams/{stream_name}")
    async def get_stream_details(stream_name: str, request: Request, _ = Depends(check_token)):
        stream = hub._streams.get(stream_name)
        if not stream:
            raise HTTPException(status_code=404, detail="Stream not found")
        
        computed_val = await stream.get()
        raw_val = await stream.get_raw()
        history_log = await stream.history(limit=20)
        
        formatted_history = []
        for item in history_log:
            formatted_history.append({
                "ts": item.get("ts"),
                "node_id": item.get("node_id"),
                "value": item.get("value"),
                "metadata": item.get("metadata", {})
            })
            
        return {
            "name": stream_name,
            "merge_rule": stream.merge_rule,
            "subscribers": len(stream._subscribers),
            "computed_value": computed_val,
            "raw_value": raw_val,
            "history": formatted_history
        }

    return router

