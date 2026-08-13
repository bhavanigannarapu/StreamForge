import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
from datetime import datetime

# Page Configuration - Wide Layout & Modern Branding
st.set_page_config(
    page_title="StreamForge — Real-Time Streaming Telemetry Control Plane",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom High-End UI/UX CSS Design System (Obsidian Dark Glassmorphism)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap');

    /* Core Application Theme */
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    .stApp {
        background: radial-gradient(circle at 50% -20%, #1a2332 0%, #0b0e14 70%);
        color: #e6edf3;
    }
    
    /* Top Header Banner */
    .brand-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 18px 24px;
        background: rgba(22, 28, 38, 0.7);
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        margin-bottom: 24px;
        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4);
    }
    
    .brand-title {
        font-size: 1.6rem;
        font-weight: 700;
        background: linear-gradient(135deg, #00f2fe 0%, #4facfe 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
        display: flex;
        align-items: center;
        gap: 10px;
    }
    
    /* Pulsing Status Dot */
    .pulse-dot {
        width: 10px;
        height: 10px;
        background-color: #00f5a0;
        border-radius: 50%;
        display: inline-block;
        box-shadow: 0 0 12px #00f5a0;
        animation: pulse-animation 1.8s infinite;
    }
    
    @keyframes pulse-animation {
        0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(0, 245, 160, 0.7); }
        70% { transform: scale(1.1); box-shadow: 0 0 0 10px rgba(0, 245, 160, 0); }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(0, 245, 160, 0); }
    }

    .badge-online {
        background: rgba(0, 245, 160, 0.12);
        color: #00f5a0;
        border: 1px solid rgba(0, 245, 160, 0.3);
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.82rem;
        font-weight: 600;
        display: inline-flex;
        align-items: center;
        gap: 6px;
    }
    
    /* Sleek Metric Cards */
    div[data-testid="stMetric"] {
        background: rgba(22, 28, 38, 0.65);
        backdrop-filter: blur(14px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 14px;
        padding: 16px 20px;
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        border-color: rgba(0, 242, 254, 0.3);
    }
    div[data-testid="stMetricLabel"] {
        font-size: 0.85rem !important;
        font-weight: 500 !important;
        color: #8b949e !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    div[data-testid="stMetricValue"] {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 2.1rem !important;
        font-weight: 700 !important;
        color: #00f2fe !important;
    }
    
    /* Alert Banners */
    .alert-banner-critical {
        background: linear-gradient(90deg, rgba(255, 65, 108, 0.2) 0%, rgba(255, 75, 43, 0.1) 100%);
        border-left: 4px solid #ff416c;
        color: #ff7b72;
        padding: 14px 18px;
        border-radius: 10px;
        margin-bottom: 12px;
        backdrop-filter: blur(10px);
        font-size: 0.95rem;
    }
    
    /* Modern Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        background-color: transparent;
        padding-bottom: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: rgba(22, 28, 38, 0.6);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 10px;
        padding: 10px 20px;
        color: #8b949e;
        font-weight: 500;
        transition: all 0.2s ease;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #00f2fe 0%, #4facfe 100%) !important;
        color: #0b0e14 !important;
        font-weight: 700 !important;
        border: none !important;
        box-shadow: 0 4px 20px rgba(0, 242, 254, 0.4);
    }
    
    /* Custom Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #0f141c;
        border-right: 1px solid rgba(255, 255, 255, 0.06);
    }
</style>
""", unsafe_allow_html=True)

# Default Backend API URL
API_URL = "http://localhost:8000"

# Header Banner HTML
st.markdown("""
<div class="brand-header">
    <div>
        <div class="brand-title">⚡ StreamForge</div>
        <div style="color: #8b949e; font-size: 0.88rem; margin-top: 2px;">
            Distributed Streaming Engine & Telemetry Control Plane
        </div>
    </div>
    <div style="display: flex; gap: 12px; align-items: center;">
        <span class="badge-online"><span class="pulse-dot"></span> LIVE ENGINE STREAM</span>
    </div>
</div>
""", unsafe_allow_html=True)

# Sidebar Controls - Modern UI
st.sidebar.markdown("### ⚙️ Engine Control")
refresh_mode = st.sidebar.selectbox(
    "Refresh Mode",
    ["Smooth Auto Stream (3s)", "Fast Stream (1s)", "Relaxed Stream (5s)", "Manual Refresh Only"]
)
api_endpoint_input = st.sidebar.text_input("FastAPI Service Endpoint", API_URL)

st.sidebar.divider()
st.sidebar.markdown("### 🧪 Anomaly Injector")
col_sb1, col_sb2 = st.sidebar.columns(2)

with col_sb1:
    if st.button("🔥 Overheat"):
        try:
            requests.post(f"{api_endpoint_input}/telemetry", json={
                "event_id": f"evt-test-overheat-{int(time.time())}",
                "truck_id": "TRUCK-005",
                "temperature": 43.8,
                "speed": 88.0,
                "timestamp": datetime.utcnow().isoformat()
            }, timeout=2)
            st.toast("🔥 Overheat anomaly injected for TRUCK-005!", icon="🔥")
        except Exception as e:
            st.error(f"Error: {e}")

with col_sb2:
    if st.button("⚠️ DLQ Error"):
        try:
            requests.post(f"{api_endpoint_input}/telemetry", json={
                "event_id": f"evt-test-dlq-{int(time.time())}",
                "truck_id": "TRUCK-008",
                "temperature": -19.5,
                "speed": 42.0,
                "timestamp": datetime.utcnow().isoformat()
            }, timeout=2)
            st.toast("⚠️ DLQ corrupt event injected!", icon="⚠️")
        except Exception as e:
            st.error(f"Error: {e}")

if st.sidebar.button("🧹 Clear Alert Logs"):
    try:
        requests.post(f"{api_endpoint_input}/alerts/clear", timeout=2)
        st.toast("Alert logs cleared", icon="🧹")
    except Exception:
        pass


# Helper Fetch Functions
def fetch_data(base_url, path):
    try:
        r = requests.get(f"{base_url}/{path}", timeout=2)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

health_data = fetch_data(api_endpoint_input, "health")
metrics_data = fetch_data(api_endpoint_input, "metrics")
alerts_data = fetch_data(api_endpoint_input, "alerts?limit=10")

# High-Level Metrics Cards
col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)

if health_data and health_data.get("status") == "ONLINE":
    col_m1.metric("API GATEWAY", "ONLINE", f"Uptime: {health_data.get('uptime_seconds')}s")
else:
    col_m1.metric("API GATEWAY", "OFFLINE", "Check Gateway")

kafka_status = "Connected" if health_data and health_data.get("kafka_connected") else "Simulation"
col_m2.metric("BROKER ENGINE", kafka_status, "truck-telemetry")

if metrics_data:
    col_m3.metric("ACTIVE FLEET", f"{metrics_data.get('active_trucks', 0)} Trucks", f"Total: {metrics_data.get('total_events', 0)}")
    col_m4.metric("FLEET AVG TEMP", f"{metrics_data.get('avg_fleet_temperature', 0)} °C", f"Range: {metrics_data.get('min_fleet_temperature', 0)}° to {metrics_data.get('max_fleet_temperature', 0)}°")
    col_m5.metric("SYSTEM ALERTS", metrics_data.get("active_alerts_count", 0), f"DLQ: {metrics_data.get('dlq_events_count', 0)}")
else:
    col_m3.metric("ACTIVE FLEET", "N/A")
    col_m4.metric("FLEET AVG TEMP", "N/A")
    col_m5.metric("SYSTEM ALERTS", "N/A")

st.markdown("<div style='margin-bottom: 18px;'></div>", unsafe_allow_html=True)

# Critical Alert Banner (if active)
if alerts_data and alerts_data.get("alerts"):
    critical_alerts = [a for a in alerts_data["alerts"] if a["severity"] == "CRITICAL"]
    if critical_alerts:
        for alt in critical_alerts[-2:]:
            st.markdown(
                f"<div class='alert-banner-critical'><b>🚨 CRITICAL ANOMALY ALERT — {alt['truck_id']}</b>: {alt['message']} "
                f"<span style='float:right; font-family: monospace;'>{alt['timestamp'][11:19]} UTC</span></div>",
                unsafe_allow_html=True
            )

# Primary Navigation Tabs
tab_analytics, tab_heatmap, tab_state, tab_alerts, tab_prom, tab_export, tab_arch = st.tabs([
    "📊 Fleet Telemetry",
    "🔥 Heat Map & GPS Map",
    "🗄️ RocksDB State Store",
    "⚠️ Alerts & DLQ Logs",
    "📈 Prometheus Metrics",
    "📥 Data Export",
    "🗺️ Architecture"
])

with tab_analytics:
    st.subheader("Live Telemetry Stream")
    telemetry_resp = fetch_data(api_endpoint_input, "telemetry?limit=100")
    
    if telemetry_resp and "data" in telemetry_resp and len(telemetry_resp["data"]) > 0:
        df = pd.DataFrame(telemetry_resp["data"])
        
        col_chart1, col_chart2 = st.columns(2)
        
        with col_chart1:
            st.markdown("##### Real-Time Temperature Stream (°C)")
            fig_temp = px.line(
                df, x="timestamp", y="temperature", color="truck_id",
                labels={"temperature": "Temp (°C)", "timestamp": "Timestamp"},
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_temp.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=360,
                margin=dict(l=10, r=10, t=30, b=10)
            )
            st.plotly_chart(fig_temp, use_container_width=True)

        with col_chart2:
            st.markdown("##### Fleet Speed Readings (km/h)")
            fig_speed = px.bar(
                df.tail(20), x="truck_id", y="speed", color="temperature",
                color_continuous_scale="Viridis",
                labels={"speed": "Speed (km/h)", "truck_id": "Truck"}
            )
            fig_speed.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=360,
                margin=dict(l=10, r=10, t=30, b=10)
            )
            st.plotly_chart(fig_speed, use_container_width=True)

        st.markdown("##### Raw Telemetry Events Feed")
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Initializing telemetry stream from engine...")

with tab_heatmap:
    st.subheader("🔥 Temperature Heat Map & 📍 Live Fleet GPS Map")
    telemetry_resp = fetch_data(api_endpoint_input, "telemetry?limit=150")
    
    if telemetry_resp and "data" in telemetry_resp and len(telemetry_resp["data"]) > 0:
        df = pd.DataFrame(telemetry_resp["data"])
        
        col_hm1, col_hm2 = st.columns(2)
        
        with col_hm1:
            st.markdown("##### Fleet Temperature Density Matrix")
            df['reading_idx'] = df.groupby('truck_id').cumcount()
            heatmap_data = df.pivot(index='truck_id', columns='reading_idx', values='temperature').fillna(0)
            
            fig_heatmap = px.imshow(
                heatmap_data,
                labels=dict(x="Reading Frame", y="Truck Identifier", color="Temp (°C)"),
                x=heatmap_data.columns,
                y=heatmap_data.index,
                color_continuous_scale="Magma",
                aspect="auto"
            )
            fig_heatmap.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=380
            )
            st.plotly_chart(fig_heatmap, use_container_width=True)
            
        with col_hm2:
            st.markdown("##### GPS Fleet Location Grid")
            if "latitude" in df.columns and "longitude" in df.columns:
                latest_gps = df.groupby("truck_id").last().reset_index()
                fig_map = px.scatter_mapbox(
                    latest_gps,
                    lat="latitude",
                    lon="longitude",
                    color="temperature",
                    size="speed",
                    hover_name="truck_id",
                    hover_data=["temperature", "speed"],
                    color_continuous_scale="Plasma",
                    zoom=5,
                    height=380
                )
                fig_map.update_layout(
                    mapbox_style="carto-darkmatter",
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=10, r=10, t=10, b=10)
                )
                st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.info("Loading GPS coordinates...")
    else:
        st.info("Loading heat map data...")

with tab_state:
    st.subheader("🗄️ RocksDB Key-Value State Viewer")
    st.caption("State engine computes rolling cumulative statistics (sum, count, average) per truck with RocksDB disk persistence.")
    state_resp = fetch_data(api_endpoint_input, "state")
    
    if state_resp and "truck_state" in state_resp:
        state_df = pd.DataFrame(list(state_resp["truck_state"].values()))
        if not state_df.empty:
            st.markdown("##### RocksDB Accumulated Fleet State Table")
            st.dataframe(state_df, use_container_width=True)
            
            fig_avg = px.bar(
                state_df, x="truck_id", y="avg_temperature", color="avg_temperature",
                color_continuous_scale="Spectral_r",
                title="Running Average Temperature by Truck",
                labels={"avg_temperature": "Avg Temp (°C)"}
            )
            fig_avg.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=350
            )
            st.plotly_chart(fig_avg, use_container_width=True)
        else:
            st.info("State store is initializing...")
    else:
        st.warning("Could not connect to state store backend.")

with tab_alerts:
    st.subheader("⚠️ Anomaly Alerts & Dead-Letter Queue (DLQ)")
    col_al1, col_al2 = st.columns(2)
    
    with col_al1:
        st.markdown("##### 🚨 Active Anomaly Alerts Log")
        if alerts_data and alerts_data.get("alerts"):
            alert_df = pd.DataFrame(alerts_data["alerts"])
            st.dataframe(alert_df, use_container_width=True)
        else:
            st.success("Zero anomaly alerts triggered.")
            
    with col_al2:
        st.markdown("##### 🚫 Dead-Letter Queue (DLQ) Intercepted Events")
        dlq_resp = fetch_data(api_endpoint_input, "dlq")
        if dlq_resp and "dlq_records" in dlq_resp and len(dlq_resp["dlq_records"]) > 0:
            dlq_df = pd.DataFrame(dlq_resp["dlq_records"])
            st.error(f"Total DLQ Events Intercepted: {len(dlq_df)}")
            st.dataframe(dlq_df, use_container_width=True)
        else:
            st.success("✅ Zero DLQ errors. All records passed validation.")

with tab_prom:
    st.subheader("📈 Prometheus Exporter Stream")
    st.caption("Standard Prometheus `/prometheus` metrics endpoint for Grafana scraping.")
    try:
        prom_resp = requests.get(f"{api_endpoint_input}/prometheus", timeout=2)
        if prom_resp.status_code == 200:
            st.code(prom_resp.text, language="text")
        else:
            st.error("Failed to fetch Prometheus metrics.")
    except Exception as e:
        st.error(f"Error connecting to Prometheus endpoint: {e}")

with tab_export:
    st.subheader("📥 Telemetry Reports & Data Export")
    st.caption("Download live telemetry records for offline reporting.")
    col_exp1, col_exp2 = st.columns(2)
    with col_exp1:
        st.markdown("##### Direct CSV Export Link")
        st.markdown(f"📥 [Click to Download Telemetry CSV]({api_endpoint_input}/telemetry/export?format=csv)")
    with col_exp2:
        st.markdown("##### Direct JSON Export Link")
        st.markdown(f"📥 [Click to Download Telemetry JSON]({api_endpoint_input}/telemetry/export?format=json)")

with tab_arch:
    st.subheader("🗺️ StreamForge System DAG Architecture")
    st.markdown("""
    ```
    ┌───────────────────────┐        ┌───────────────────────┐        ┌───────────────────────┐
    │  IoT Truck Producer   │ ─────► │   Apache Kafka        │ ─────► │   Stream Worker Node  │
    │  (producer.py)        │        │   (truck-telemetry)   │        │   (worker.py)         │
    └───────────────────────┘        └───────────────────────┘        └───────────┬───────────┘
                                                                                  │
                                     ┌───────────────────────┐                    ▼
                                     │  FastAPI Backend API  │ ◄───────── ┌───────────────┐
                                     │  (api/main.py)        │            │ RocksDB Store │
                                     └───────────┬───────────┘            └───────────────┘
                                                 │
                                                 ▼
                                     ┌───────────────────────┐
                                     │ Streamlit Dashboard   │
                                     │ (dashboard/app.py)    │
                                     └───────────┬───────────┘
    ```
    """)

# Smooth Auto-Refresh Control
if refresh_mode == "Fast Stream (1s)":
    time.sleep(1)
    st.rerun()
elif refresh_mode == "Smooth Auto Stream (3s)":
    time.sleep(3)
    st.rerun()
elif refresh_mode == "Relaxed Stream (5s)":
    time.sleep(5)
    st.rerun()
