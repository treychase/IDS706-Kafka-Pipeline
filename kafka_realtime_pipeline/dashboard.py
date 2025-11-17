import time
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="IoT Smart Building Dashboard", layout="wide", page_icon="🏢")

# Custom CSS
st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    </style>
""", unsafe_allow_html=True)

st.title("🏢 IoT Smart Building Monitoring Dashboard")
st.markdown("Real-time sensor data with anomaly detection and predictive analytics")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"

@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)

engine = get_engine(DATABASE_URL)

def load_sensor_data(building_filter: str | None = None, status_filter: str | None = None, limit: int = 500) -> pd.DataFrame:
    """Load sensor readings from database."""
    base_query = "SELECT * FROM sensor_readings"
    conditions = []
    params = {}
    
    if building_filter and building_filter != "All":
        conditions.append("building = :building")
        params["building"] = building_filter
    
    if status_filter and status_filter != "All":
        conditions.append("status = :status")
        params["status"] = status_filter
    
    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)
    
    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = limit

    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        if not df.empty and "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        st.error(f"Error loading sensor data: {e}")
        return pd.DataFrame()

def load_anomalies(limit: int = 100) -> pd.DataFrame:
    """Load detected anomalies."""
    query = f"""
        SELECT * FROM sensor_anomalies 
        ORDER BY detected_at DESC 
        LIMIT {limit}
    """
    try:
        df = pd.read_sql_query(query, engine)
        if not df.empty and "detected_at" in df.columns:
            df["detected_at"] = pd.to_datetime(df["detected_at"])
        return df
    except Exception as e:
        st.error(f"Error loading anomalies: {e}")
        return pd.DataFrame()

def load_aggregates(limit: int = 100) -> pd.DataFrame:
    """Load Flink aggregated data."""
    query = f"""
        SELECT * FROM sensor_aggregates 
        ORDER BY created_at DESC 
        LIMIT {limit}
    """
    try:
        df = pd.read_sql_query(query, engine)
        if not df.empty:
            if "created_at" in df.columns:
                df["created_at"] = pd.to_datetime(df["created_at"])
            if "window_start" in df.columns:
                df["window_start"] = pd.to_datetime(df["window_start"])
        return df
    except Exception as e:
        st.error(f"Error loading aggregates: {e}")
        return pd.DataFrame()

# Sidebar controls
st.sidebar.header("⚙️ Dashboard Controls")

building_options = ["All", "Building A", "Building B", "Building C"]
selected_building = st.sidebar.selectbox("🏢 Filter by Building", building_options)

status_options = ["All", "Normal", "Warning", "Critical", "Maintenance"]
selected_status = st.sidebar.selectbox("📊 Filter by Status", status_options)

update_interval = st.sidebar.slider("🔄 Update Interval (seconds)", min_value=3, max_value=30, value=10)
limit_records = st.sidebar.number_input("📋 Number of records to load", min_value=100, max_value=5000, value=500, step=100)

show_anomalies = st.sidebar.checkbox("🚨 Show Anomalies", value=True)
show_aggregates = st.sidebar.checkbox("📊 Show Flink Aggregates", value=True)

if st.sidebar.button("🔄 Refresh Now"):
    st.rerun()

# Main dashboard loop
placeholder = st.empty()

while True:
    df_sensors = load_sensor_data(selected_building, selected_status, limit=int(limit_records))
    
    with placeholder.container():
        if df_sensors.empty:
            st.warning("⏳ No sensor data available. Waiting for data stream...")
            time.sleep(update_interval)
            continue

        # === KEY METRICS ===
        st.subheader(f"📈 Real-Time Metrics ({len(df_sensors)} readings)")
        
        # Calculate KPIs
        avg_temp = df_sensors["temperature"].mean()
        avg_humidity = df_sensors["humidity"].mean()
        avg_co2 = df_sensors["co2"].mean()
        total_occupancy = df_sensors["occupancy"].sum()
        total_energy = df_sensors["energy_consumption"].sum()
        
        # Count by status
        normal_count = len(df_sensors[df_sensors["status"] == "Normal"])
        warning_count = len(df_sensors[df_sensors["status"] == "Warning"])
        critical_count = len(df_sensors[df_sensors["status"] == "Critical"])
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("🌡️ Avg Temperature", f"{avg_temp:.1f}°C", 
                     delta=None if len(df_sensors) < 2 else f"{avg_temp - df_sensors['temperature'].iloc[-1]:.1f}°C")
        with col2:
            st.metric("💧 Avg Humidity", f"{avg_humidity:.1f}%")
        with col3:
            st.metric("🫁 Avg CO2", f"{avg_co2:.0f} ppm")
        with col4:
            st.metric("👥 Total Occupancy", f"{int(total_occupancy)}")
        with col5:
            st.metric("⚡ Total Energy", f"{total_energy:.1f} kWh")
        
        col6, col7, col8 = st.columns(3)
        with col6:
            st.metric("🟢 Normal", normal_count)
        with col7:
            st.metric("🟡 Warnings", warning_count)
        with col8:
            st.metric("🔴 Critical", critical_count)

        # === CHARTS ===
        st.markdown("---")
        
        # Time series of temperature and energy
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            if len(df_sensors) > 0:
                fig_temp_time = px.line(
                    df_sensors.sort_values("timestamp").tail(50),
                    x="timestamp",
                    y="temperature",
                    color="building",
                    title="🌡️ Temperature Over Time (Last 50 readings)"
                )
                fig_temp_time.update_layout(height=300)
                st.plotly_chart(fig_temp_time, use_container_width=True)
        
        with chart_col2:
            if len(df_sensors) > 0:
                fig_energy_time = px.line(
                    df_sensors.sort_values("timestamp").tail(50),
                    x="timestamp",
                    y="energy_consumption",
                    color="building",
                    title="⚡ Energy Consumption Over Time"
                )
                fig_energy_time.update_layout(height=300)
                st.plotly_chart(fig_energy_time, use_container_width=True)
        
        # Distribution charts
        chart_col3, chart_col4 = st.columns(2)
        
        with chart_col3:
            # Energy by building and floor
            energy_by_location = df_sensors.groupby(["building", "floor"])["energy_consumption"].sum().reset_index()
            fig_energy = px.bar(
                energy_by_location.sort_values("energy_consumption", ascending=False).head(10),
                x="building",
                y="energy_consumption",
                color="floor",
                title="⚡ Energy Consumption by Building & Floor",
                barmode="group"
            )
            fig_energy.update_layout(height=300)
            st.plotly_chart(fig_energy, use_container_width=True)
        
        with chart_col4:
            # Room type analysis
            room_stats = df_sensors.groupby("room_type").agg({
                "temperature": "mean",
                "energy_consumption": "sum"
            }).reset_index()
            
            fig_room = px.scatter(
                room_stats,
                x="temperature",
                y="energy_consumption",
                size="energy_consumption",
                color="room_type",
                title="🏠 Room Type Analysis: Temp vs Energy",
                hover_data=["room_type"]
            )
            fig_room.update_layout(height=300)
            st.plotly_chart(fig_room, use_container_width=True)
        
        # Status distribution
        chart_col5, chart_col6 = st.columns(2)
        
        with chart_col5:
            status_dist = df_sensors["status"].value_counts().reset_index()
            status_dist.columns = ["status", "count"]
            fig_status = px.pie(
                status_dist,
                values="count",
                names="status",
                title="📊 Sensor Status Distribution",
                color="status",
                color_discrete_map={"Normal": "green", "Warning": "orange", "Critical": "red", "Maintenance": "blue"}
            )
            fig_status.update_layout(height=300)
            st.plotly_chart(fig_status, use_container_width=True)
        
        with chart_col6:
            # CO2 levels by room type
            co2_by_room = df_sensors.groupby("room_type")["co2"].mean().reset_index().sort_values("co2", ascending=False)
            fig_co2 = px.bar(
                co2_by_room,
                x="room_type",
                y="co2",
                title="🫁 Average CO2 Levels by Room Type",
                color="co2",
                color_continuous_scale="Reds"
            )
            fig_co2.update_layout(height=300)
            st.plotly_chart(fig_co2, use_container_width=True)

        # === ANOMALIES SECTION ===
        if show_anomalies:
            st.markdown("---")
            st.subheader("🚨 Recent Anomalies Detected")
            
            df_anomalies = load_anomalies(limit=50)
            
            if not df_anomalies.empty:
                # Anomaly metrics
                anomaly_col1, anomaly_col2, anomaly_col3 = st.columns(3)
                
                with anomaly_col1:
                    st.metric("Total Anomalies", len(df_anomalies))
                with anomaly_col2:
                    high_severity = len(df_anomalies[df_anomalies["anomaly_score"] > 0.7])
                    st.metric("High Severity (>0.7)", high_severity)
                with anomaly_col3:
                    unique_rooms = df_anomalies["room_id"].nunique()
                    st.metric("Affected Rooms", unique_rooms)
                
                # Anomaly type distribution
                anomaly_chart_col1, anomaly_chart_col2 = st.columns(2)
                
                with anomaly_chart_col1:
                    anomaly_types = df_anomalies["anomaly_type"].value_counts().reset_index()
                    anomaly_types.columns = ["type", "count"]
                    fig_anomaly_types = px.bar(
                        anomaly_types.head(10),
                        x="count",
                        y="type",
                        orientation="h",
                        title="🔍 Anomaly Types",
                        color="count",
                        color_continuous_scale="Reds"
                    )
                    st.plotly_chart(fig_anomaly_types, use_container_width=True)
                
                with anomaly_chart_col2:
                    # Anomaly severity over time
                    if "detected_at" in df_anomalies.columns:
                        df_anomalies_sorted = df_anomalies.sort_values("detected_at")
                        fig_anomaly_time = px.scatter(
                            df_anomalies_sorted.tail(30),
                            x="detected_at",
                            y="anomaly_score",
                            color="anomaly_type",
                            title="📈 Anomaly Severity Timeline",
                            size="anomaly_score"
                        )
                        st.plotly_chart(fig_anomaly_time, use_container_width=True)
                
                # Recent anomalies table
                st.markdown("#### Recent Anomalies (Top 10)")
                display_columns = ["room_id", "anomaly_type", "anomaly_score", "temperature", "humidity", "co2", "energy_consumption", "detected_at"]
                available_columns = [col for col in display_columns if col in df_anomalies.columns]
                st.dataframe(
                    df_anomalies[available_columns].head(10),
                    use_container_width=True
                )
            else:
                st.info("✅ No anomalies detected recently")

        # === FLINK AGGREGATES SECTION ===
        if show_aggregates:
            st.markdown("---")
            st.subheader("📊 Flink Real-Time Aggregations")
            
            df_aggregates = load_aggregates(limit=50)
            
            if not df_aggregates.empty:
                agg_col1, agg_col2 = st.columns(2)
                
                with agg_col1:
                    # Average metrics by building
                    fig_agg_temp = px.bar(
                        df_aggregates.groupby("building")["avg_temperature"].mean().reset_index(),
                        x="building",
                        y="avg_temperature",
                        title="🌡️ Average Temperature by Building (Aggregated)",
                        color="avg_temperature",
                        color_continuous_scale="RdYlBu_r"
                    )
                    st.plotly_chart(fig_agg_temp, use_container_width=True)
                
                with agg_col2:
                    # Total energy by building
                    fig_agg_energy = px.bar(
                        df_aggregates.groupby("building")["total_energy"].sum().reset_index(),
                        x="building",
                        y="total_energy",
                        title="⚡ Total Energy by Building (Aggregated)",
                        color="total_energy",
                        color_continuous_scale="Viridis"
                    )
                    st.plotly_chart(fig_agg_energy, use_container_width=True)
                
                st.markdown("#### Recent Aggregations (Top 10)")
                display_agg_cols = ["building", "floor", "avg_temperature", "avg_humidity", "avg_co2", "total_occupancy", "total_energy", "reading_count", "created_at"]
                available_agg_cols = [col for col in display_agg_cols if col in df_aggregates.columns]
                st.dataframe(
                    df_aggregates[available_agg_cols].head(10),
                    use_container_width=True
                )
            else:
                st.info("⏳ No aggregated data available yet")

        # === RAW DATA ===
        st.markdown("---")
        st.subheader("📋 Recent Sensor Readings (Top 15)")
        
        display_sensor_cols = ["room_id", "building", "floor", "room_type", "temperature", "humidity", "co2", "occupancy", "energy_consumption", "status", "timestamp"]
        available_sensor_cols = [col for col in display_sensor_cols if col in df_sensors.columns]
        st.dataframe(
            df_sensors[available_sensor_cols].head(15),
            use_container_width=True
        )

        # Footer
        st.markdown("---")
        st.caption(f"🕐 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Auto-refresh: {update_interval}s | Filter: {selected_building} / {selected_status}")

    time.sleep(update_interval)