import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
from datetime import datetime

# Page Configuration
st.set_page_config(
    page_title="StreamForge - Enterprise Observability Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Premium Dark Glassmorphism Design CSS
st.markdown("""
<style>
    /* Dark Theme Core */
    .stApp {
        background-color: #0d1117;
        color: #c9d1d9;
    }
    
    /* Glassmorphism Card Styling */
    div[data-testid="stMetricValue"] {
        font-size: 2.2rem !important;
        font-weight: 700 !important;
        color: #58a6ff !important;
    }
    
    .glass-card {
        background: rgba(22, 27, 34, 0.75);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 15px;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
    }
    
    .alert-banner-critical {
        background: rgba(248, 81, 73, 0.15);
        border-left: 4px solid #f85149;
        color: #ff7b72;
        padding: 12px 16px;
        border-radius: 8px;
        margin-bottom: 10px;
    }

    .alert-banner-warning {
        background: rgba(210, 153, 34, 0.15);
        border-left: 4px solid #d29922;
        color: #e3b341;
        padding: 12px 16px;
        border-radius: 8px;
        margin-bottom: 10px;
    }
    
    /* Custom Tab Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #161b22;
        border-radius: 8px;
        padding: 8px 16px;
        color: #8b949e;
    }
    .stTabs [aria-selected="true"] {
        background-color: #1f6feb !important;
        color: #ffffff !important;
    }
</style>
""", unsafe_allow_html=True)

# Default Backend API URL
API_URL = "http://localhost:8000"

st.title("⚡ StreamForge - Enterprise Stream Observability Platform")
st.caption("Real-Time Fleet Telemetry, RocksDB State Manager, Prometheus Metrics & Heat Map Analytics")

# Sidebar Controls
st.sidebar.header("⚙️ System Control & Settings")
auto_refresh = st.sidebar.checkbox("Auto Refresh (2s)", value=True)
api_endpoint_input = st.sidebar.text_input("FastAPI Base URL", API_URL)

st.sidebar.divider()
st.sidebar.markdown("### 🧪 Quick Actions")
if st.sidebar.button("🔥 Inject Overheat Anomaly (+42°C)"):
    try:
        requests.post(f"{api_endpoint_input}/telemetry", json={
            "event_id": f"evt-test-overheat-{int(time.time())}",
            "truck_id": "TRUCK-005",
            "temperature": 42.5,
            "speed": 85.0,
            "timestamp": datetime.utcnow().isoformat()
        }, timeout=2)
        st.sidebar.success("Injected overheat event for TRUCK-005!")
    except Exception as e:
        st.sidebar.error(f"Failed to inject event: {e}")

if st.sidebar.button("⚠️ Inject DLQ Corrupt Event (-18°C)"):
    try:
        requests.post(f"{api_endpoint_input}/telemetry", json={
            "event_id": f"evt-test-dlq-{int(time.time())}",
            "truck_id": "TRUCK-008",
            "temperature": -18.2,
            "speed": 40.0,
            "timestamp": datetime.utcnow().isoformat()
        }, timeout=2)
        st.sidebar.warning("Injected DLQ event for TRUCK-008!")
    except Exception as e:
        st.sidebar.error(f"Failed to inject DLQ event: {e}")

if st.sidebar.button("🧹 Clear Alerts History"):
    try:
        requests.post(f"{api_endpoint_input}/alerts/clear", timeout=2)
        st.sidebar.info("Alert history cleared.")
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

# Top Metrics Ribbon
col_h1, col_h2, col_h3, col_h4, col_h5 = st.columns(5)

if health_data and health_data.get("status") == "ONLINE":
    col_h1.metric("API Service", "🟢 ONLINE", f"v{health_data.get('version', '2.0')}")
else:
    col_h1.metric("API Service", "🔴 OFFLINE", "Check FastAPI")

kafka_str = "🟢 Kafka Broker" if health_data and health_data.get("kafka_connected") else "⚡ Standalone Sim"
col_h2.metric("Broker Engine", kafka_str, "truck-telemetry")

if metrics_data:
    col_h3.metric("Active Fleet", metrics_data.get("active_trucks", 0), f"Events: {metrics_data.get('total_events', 0)}")
    col_h4.metric("Avg Fleet Temp", f"{metrics_data.get('avg_fleet_temperature', 0)} °C", f"Min: {metrics_data.get('min_fleet_temperature', 0)}°C")
    col_h5.metric("Active Alerts", metrics_data.get("active_alerts_count", 0), f"DLQ: {metrics_data.get('dlq_events_count', 0)}")
else:
    col_h3.metric("Active Fleet", "N/A")
    col_h4.metric("Avg Fleet Temp", "N/A")
    col_h5.metric("Active Alerts", "N/A")

st.divider()

# Active Critical Alert Banners
if alerts_data and alerts_data.get("alerts"):
    critical_alerts = [a for a in alerts_data["alerts"] if a["severity"] == "CRITICAL"]
    if critical_alerts:
        st.markdown("### 🚨 Critical Fleet Alerts")
        for alt in critical_alerts[-3:]:
            st.markdown(
                f"<div class='alert-banner-critical'><b>[CRITICAL] {alt['truck_id']}</b>: {alt['message']} "
                f"<span style='float:right;'>{alt['timestamp'][11:19]}</span></div>",
                unsafe_allow_html=True
            )

# Navigation Tabs
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📊 Live Fleet Analytics",
    "🔥 Heat Map & GPS Map",
    "🗄️ RocksDB State Engine",
    "⚠️ DLQ & Alerts Log",
    "📈 Prometheus Metrics",
    "📥 Export & Reports",
    "🛠️ System Architecture"
])

with tab1:
    st.subheader("Real-Time Fleet Telemetry Stream")
    telemetry_resp = fetch_data(api_endpoint_input, "telemetry?limit=100")
    
    if telemetry_resp and "data" in telemetry_resp and len(telemetry_resp["data"]) > 0:
        df = pd.DataFrame(telemetry_resp["data"])
        
        col_c1, col_c2 = st.columns(2)
        with col_c1:
            st.markdown("##### Temperature Trend Stream (°C)")
            fig_temp = px.line(
                df, x="timestamp", y="temperature", color="truck_id",
                labels={"temperature": "Temp (°C)", "timestamp": "Timestamp"},
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            fig_temp.update_layout(template="plotly_dark", height=360, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_temp, use_container_width=True)

        with col_c2:
            st.markdown("##### Fleet Speed Distribution (km/h)")
            fig_speed = px.bar(
                df.tail(20), x="truck_id", y="speed", color="temperature",
                color_continuous_scale="Reds",
                labels={"speed": "Speed (km/h)", "truck_id": "Truck"}
            )
            fig_speed.update_layout(template="plotly_dark", height=360, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_speed, use_container_width=True)

        st.markdown("##### Live Telemetry Stream Data Table")
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Waiting for incoming telemetry events from Kafka / Backend...")

with tab2:
    st.subheader("🔥 Fleet Temperature Heat Map & 📍 GPS Route Locations")
    telemetry_resp = fetch_data(api_endpoint_input, "telemetry?limit=150")
    
    if telemetry_resp and "data" in telemetry_resp and len(telemetry_resp["data"]) > 0:
        df = pd.DataFrame(telemetry_resp["data"])
        
        col_hm1, col_hm2 = st.columns(2)
        
        with col_hm1:
            st.markdown("##### Fleet Temperature Density Heat Map")
            # Pivot table for Heat Map (Truck ID vs Reading Index)
            df['reading_idx'] = df.groupby('truck_id').cumcount()
            heatmap_data = df.pivot(index='truck_id', columns='reading_idx', values='temperature').fillna(0)
            
            fig_heatmap = px.imshow(
                heatmap_data,
                labels=dict(x="Reading Index", y="Truck ID", color="Temp (°C)"),
                x=heatmap_data.columns,
                y=heatmap_data.index,
                color_continuous_scale="Viridis",
                aspect="auto"
            )
            fig_heatmap.update_layout(template="plotly_dark", height=380)
            st.plotly_chart(fig_heatmap, use_container_width=True)
            
        with col_hm2:
            st.markdown("##### Live GPS Fleet Location Map")
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
                    color_continuous_scale="Portland",
                    zoom=5,
                    height=380
                )
                fig_map.update_layout(
                    mapbox_style="carto-darkmatter",
                    template="plotly_dark",
                    margin=dict(l=10, r=10, t=10, b=10)
                )
                st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.info("GPS coordinates initializing...")
    else:
        st.info("Fetching telemetry data for heat map...")

with tab3:
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
            fig_avg.update_layout(template="plotly_dark", height=350)
            st.plotly_chart(fig_avg, use_container_width=True)
        else:
            st.info("State store is initializing.")
    else:
        st.warning("Could not connect to state store backend.")

with tab4:
    st.subheader("⚠️ Dead Letter Queue (DLQ) & Alert Logs")
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
            st.error(f"Total DLQ Intercepted Events: {len(dlq_df)}")
            st.dataframe(dlq_df, use_container_width=True)
        else:
            st.success("✅ Zero DLQ errors. All records passed validation.")

with tab5:
    st.subheader("📈 Prometheus Metrics & Observability")
    st.caption("Standard Prometheus `/prometheus` scraper metrics endpoint for Grafana integration.")
    
    try:
        prom_resp = requests.get(f"{api_endpoint_input}/prometheus", timeout=2)
        if prom_resp.status_code == 200:
            st.code(prom_resp.text, language="text")
        else:
            st.error("Failed to fetch Prometheus metrics.")
    except Exception as e:
        st.error(f"Error connecting to Prometheus endpoint: {e}")

with tab6:
    st.subheader("📥 Telemetry Data & Reports Export")
    st.caption("Download real-time telemetry records in CSV format for offline reporting.")
    
    col_exp1, col_exp2 = st.columns(2)
    with col_exp1:
        st.markdown("##### Direct CSV Export Link")
        st.markdown(f"📥 [Click to Download Telemetry CSV]({api_endpoint_input}/telemetry/export?format=csv)")
    with col_exp2:
        st.markdown("##### Direct JSON Export Link")
        st.markdown(f"📥 [Click to Download Telemetry JSON]({api_endpoint_input}/telemetry/export?format=json)")

with tab7:
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
                                     └───────────────────────┘
    ```
    """)

if auto_refresh:
    time.sleep(2)
    st.rerun()
