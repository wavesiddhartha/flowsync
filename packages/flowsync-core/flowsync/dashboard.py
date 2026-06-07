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
            margin-bottom: 3rem;
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
            margin-bottom: 3rem;
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

    <script>
        function formatUptime(secs) {
            const h = Math.floor(secs / 3600).toString().padStart(2, '0');
            const m = Math.floor((secs % 3600) / 60).toString().padStart(2, '0');
            const s = (secs % 60).toString().padStart(2, '0');
            return `Uptime: ${h}:${m}:${s}`;
        }
        function escapeHtml(str) {
            const div = document.createElement('div');
            div.textContent = str;
            return div.innerHTML;
        }

        async function fetchStats() {
            try {
                // Dynamically resolve endpoint relative to current page path
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
                
                const tbody = document.getElementById('streams-table-body');
                if (data.streams.length === 0) {
                    tbody.innerHTML = `<tr><td colspan="3" style="text-align: center; color: var(--text-secondary);">No streams active</td></tr>`;
                } else {
                    tbody.innerHTML = data.streams.map(s => `
                        <tr>
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

        setInterval(fetchStats, 1000);
        fetchStats();
    </script>
</body>
</html>

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

    return router
