import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time

st.set_page_config(
    page_title="StreamForge - Real-Time Telemetry Dashboard",
    page_icon="⚡",
    layout="wide"
)

# API Endpoint URL
API_URL = "http://localhost:8000"

st.title("⚡ StreamForge - Distributed Stream Processing Engine")
st.caption("Real-Time IoT Fleet Telemetry, RocksDB State Viewer & Distributed System Health")
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
    }
    /* Prevents Streamlit from dimming/fading the UI during auto-refresh */
    .stAppViewContainer {
        opacity: 1 !important;
    }
    /* Smooths out transition effects */
    div[data-testid="stAppViewBlockContainer"] {
        transition: opacity 0.1s ease-in-out;
    }
</style>
""", unsafe_allow_html=True)
# Sidebar - Settings & Service Control
st.sidebar.header("⚙️ Control Panel")
auto_refresh = st.sidebar.checkbox("Auto Refresh (2s)", value=True)
api_endpoint_input = st.sidebar.text_input("FastAPI Service URL", API_URL)

# Fetch System Health
@st.cache_data(ttl=1)
def fetch_health(base_url):
    try:
        r = requests.get(f"{base_url}/health", timeout=2)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

@st.cache_data(ttl=1)
def fetch_data(base_url, path):
    try:
        r = requests.get(f"{base_url}/{path}", timeout=2)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

health_data = fetch_health(api_endpoint_input)

# Top Bar - Status Indicators
col_h1, col_h2, col_h3, col_h4 = st.columns(4)

if health_data and health_data.get("status") == "ONLINE":
    col_h1.metric("Backend Status", "🟢 ONLINE", f"Uptime: {health_data.get('uptime_seconds')}s")
else:
    col_h1.metric("Backend Status", "🔴 OFFLINE", "Check FastAPI server")

kafka_status = "🟢 Connected" if health_data and health_data.get("kafka_connected") else "⚡ Simulation Mode"
col_h2.metric("Kafka Broker", kafka_status, "Topic: truck-telemetry")

# Fetch Metrics
metrics = fetch_data(api_endpoint_input, "metrics")
if metrics:
    col_h3.metric("Active Fleet Trucks", metrics.get("active_trucks", 0), f"Total Events: {metrics.get('total_events', 0)}")
    col_h4.metric("Avg Fleet Temp", f"{metrics.get('avg_fleet_temperature', 0)} °C", f"DLQ Events: {metrics.get('dlq_events_count', 0)}")
else:
    col_h3.metric("Active Fleet Trucks", "N/A")
    col_h4.metric("Avg Fleet Temp", "N/A")

st.divider()

# Navigation Tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Real-Time Analytics", 
    "🗄️ RocksDB State Store", 
    "⚠️ Dead Letter Queue (DLQ)", 
    "🗺️ Stream Topology DAG",
    "🔌 API Inspector"
])

with tab1:
    st.subheader("Fleet Telemetry Stream")
    telemetry_resp = fetch_data(api_endpoint_input, "telemetry?limit=100")
    
    if telemetry_resp and "data" in telemetry_resp and len(telemetry_resp["data"]) > 0:
        df = pd.DataFrame(telemetry_resp["data"])
        
        # Split layout for charts
        col_c1, col_c2 = st.columns(2)
        
        with col_c1:
            st.markdown("##### Real-Time Temperature Stream (°C)")
            fig_temp = px.line(
                df, x="timestamp", y="temperature", color="truck_id",
                title="Truck Temperature Monitoring",
                labels={"temperature": "Temperature (°C)", "timestamp": "Time"}
            )
            fig_temp.update_layout(template="plotly_dark", height=350)
            st.plotly_chart(fig_temp, use_container_width=True)
            
        with col_c2:
            st.markdown("##### Fleet Speed Distribution (km/h)")
            fig_speed = px.bar(
                df.tail(20), x="truck_id", y="speed", color="truck_id",
                title="Latest Speed Readings by Truck",
                labels={"speed": "Speed (km/h)"}
            )
            fig_speed.update_layout(template="plotly_dark", height=350)
            st.plotly_chart(fig_speed, use_container_width=True)
            
        st.markdown("##### Raw Telemetry Events Log")
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Waiting for incoming telemetry events from Kafka / Backend...")

with tab2:
    st.subheader("RocksDB / Application State Viewer")
    state_resp = fetch_data(api_endpoint_input, "state")
    if state_resp and "truck_state" in state_resp:
        state_df = pd.DataFrame(list(state_resp["truck_state"].values()))
        if not state_df.empty:
            st.markdown("##### Accumulated Fleet Running Averages & Max/Min States")
            st.dataframe(state_df, use_container_width=True)
            
            fig_avg = px.bar(
                state_df, x="truck_id", y="avg_temperature", color="avg_temperature",
                color_continuous_scale="Reds",
                title="Running Average Temperature by Truck",
                labels={"avg_temperature": "Avg Temp (°C)"}
            )
            fig_avg.update_layout(template="plotly_dark", height=350)
            st.plotly_chart(fig_avg, use_container_width=True)
        else:
            st.info("State store is currently empty.")
    else:
        st.warning("Could not fetch state store data from backend API.")

with tab3:
    st.subheader("Dead Letter Queue (DLQ) Logs")
    st.caption("Captures corrupt messages, missing fields, or temperatures out of valid boundaries (< -5°C)")
    dlq_resp = fetch_data(api_endpoint_input, "dlq")
    if dlq_resp and "dlq_records" in dlq_resp and len(dlq_resp["dlq_records"]) > 0:
        dlq_df = pd.DataFrame(dlq_resp["dlq_records"])
        st.error(f"⚠️ Total DLQ Records Intercepted: {len(dlq_df)}")
        st.dataframe(dlq_df, use_container_width=True)
    else:
        st.success("✅ Zero DLQ errors detected! All stream records are valid.")

with tab4:
    st.subheader("StreamForge System DAG Architecture")
    st.markdown("""
    ```
    ┌───────────────────────┐        ┌───────────────────────┐        ┌───────────────────────┐
    │  IoT Truck Producer   │ ─────► │   Apache Kafka        │ ─────► │   Stream Worker Node  │
    │  (producer.py)        │        │   (truck-telemetry)   │        │   (worker.py)         │
    └───────────────────────┘        └───────────────────────┘        └───────────┬───────────┘
                                                                                  │
                                     ┌───────────────────────┐                    ▼
                                     │  FastAPI Backend      │ ◄───────── ┌───────────────┐
                                     │  (api/main.py)        │            │ RocksDB Store │
                                     └───────────┬───────────┘            └───────────────┘
                                                 │
                                                 ▼
                                     ┌───────────────────────┐
                                     │  Streamlit / React UI │
                                     │  (dashboard/app.py)   │
                                     └───────────────────────┘
    ```
    """)

with tab5:
    st.subheader("FastAPI REST Endpoints Test Console")
    st.markdown(f"- **Health Check:** [{api_endpoint_input}/health]({api_endpoint_input}/health)")
    st.markdown(f"- **Telemetry Stream:** [{api_endpoint_input}/telemetry]({api_endpoint_input}/telemetry)")
    st.markdown(f"- **Telemetre Alias:** [{api_endpoint_input}/telemetre]({api_endpoint_input}/telemetre)")
    st.markdown(f"- **System Metrics:** [{api_endpoint_input}/metrics]({api_endpoint_input}/metrics)")
    st.markdown(f"- **State Store:** [{api_endpoint_input}/state]({api_endpoint_input}/state)")
    st.markdown(f"- **DLQ Log:** [{api_endpoint_input}/dlq]({api_endpoint_input}/dlq)")
    st.markdown(f"- **Interactive Swagger Docs:** [{api_endpoint_input}/docs]({api_endpoint_input}/docs)")
if auto_refresh:
    time.sleep(4)
    st.rerun()
