import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient
from src.utils.config_loader import get_config

st.set_page_config(
    page_title="AirGuard Dashboard",
    page_icon="",
    layout="wide"
)

config = get_config()
cfg = config["influxdb"]


@st.cache_resource
def get_client():
    return InfluxDBClient(
        url=cfg["url"],
        token=cfg["token"],
        org=cfg["org"]
    )


def query_data(hours: int = 1) -> pd.DataFrame:
    client = get_client()
    query_api = client.query_api()
    query = f'''
    from(bucket: "air_quality")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r._measurement == "air_quality")
      |> filter(fn: (r) => r._field == "aqi_value" or
                           r._field == "mq135_ppm" or
                           r._field == "mq7_ppm" or
                           r._field == "temperature_c" or
                           r._field == "humidity_pct")
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    try:
        tables = query_api.query_data_frame(query)
        if isinstance(tables, list):
            df = pd.concat(tables, ignore_index=True)
        else:
            df = tables
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns={"_time": "timestamp"})
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        cols = ["timestamp", "aqi_value", "mq135_ppm",
                "mq7_ppm", "temperature_c", "humidity_pct"]
        cols = [c for c in cols if c in df.columns]
        return df[cols].sort_values("timestamp")
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


def query_scenarios() -> pd.DataFrame:
    client = get_client()
    query_api = client.query_api()
    query = '''
    from(bucket: "air_quality")
      |> range(start: -24h)
      |> filter(fn: (r) => r._measurement == "air_quality")
      |> filter(fn: (r) => r._field == "aqi_value")
      |> keep(columns: ["_time", "_value", "scenario", "aqi_category"])
    '''
    try:
        tables = query_api.query_data_frame(query)
        if isinstance(tables, list):
            df = pd.concat(tables, ignore_index=True)
        else:
            df = tables
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns={"_time": "timestamp", "_value": "aqi_value"})
        return df
    except Exception as e:
        return pd.DataFrame()


def query_category_counts() -> pd.DataFrame:
    client = get_client()
    query_api = client.query_api()
    query = '''
    from(bucket: "air_quality")
      |> range(start: -24h)
      |> filter(fn: (r) => r._measurement == "air_quality")
      |> filter(fn: (r) => r._field == "aqi_value")
      |> group(columns: ["aqi_category"])
      |> count()
    '''
    try:
        tables = query_api.query_data_frame(query)
        if isinstance(tables, list):
            df = pd.concat(tables, ignore_index=True)
        else:
            df = tables
        if df.empty:
            return pd.DataFrame()
        return df[["aqi_category", "_value"]].rename(columns={"_value": "count"})
    except Exception as e:
        return pd.DataFrame()


# ── Page Header ────────────────────────────────────────────────────────────────
st.title("AirGuard — Air Quality Monitor")
st.markdown("Real-time indoor air quality monitoring dashboard")

# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.header("Settings")
hours = st.sidebar.slider("Data range (hours)", 1, 24, 1)
auto_refresh = st.sidebar.checkbox("Auto refresh (30s)", value=True)

if auto_refresh:
    st.sidebar.info("Dashboard refreshes every 30 seconds")

st.sidebar.markdown("---")
st.sidebar.markdown("**AQI Health Guide**")
st.sidebar.markdown("0-50: Good")
st.sidebar.markdown("51-100: Moderate")
st.sidebar.markdown("101-150: Unhealthy for Sensitive Groups")
st.sidebar.markdown("151-200: Unhealthy")
st.sidebar.markdown("201-300: Very Unhealthy")
st.sidebar.markdown("300+: Hazardous")

# ── Load Data ──────────────────────────────────────────────────────────────────
df = query_data(hours)

if df.empty:
    st.warning("No data found. Make sure the pipeline and simulator are running.")
    st.info("Run: python scripts/run_pipeline.py  and  python scripts/run_simulator.py")
    st.stop()

# ── Metric Cards ───────────────────────────────────────────────────────────────
st.subheader("Current Readings")
col1, col2, col3, col4, col5 = st.columns(5)

latest = df.iloc[-1]

with col1:
    aqi = latest.get("aqi_value", 0)
    st.metric("AQI", f"{aqi:.1f}" if aqi else "N/A")

with col2:
    mq135 = latest.get("mq135_ppm", 0)
    st.metric("MQ-135 (ppm)", f"{mq135:.1f}" if mq135 else "N/A")

with col3:
    mq7 = latest.get("mq7_ppm", 0)
    st.metric("MQ-7 CO (ppm)", f"{mq7:.1f}" if mq7 else "N/A")

with col4:
    temp = latest.get("temperature_c", 0)
    st.metric("Temperature (C)", f"{temp:.1f}" if temp else "N/A")

with col5:
    hum = latest.get("humidity_pct", 0)
    st.metric("Humidity (%)", f"{hum:.1f}" if hum else "N/A")

st.divider()

# ── Visualization 1: Live Trend ────────────────────────────────────────────────
st.subheader("Visualization 1 — Live Sensor Trends Over Time")

fig1 = go.Figure()

if "aqi_value" in df.columns:
    fig1.add_trace(go.Scatter(
        x=df["timestamp"], y=df["aqi_value"],
        name="AQI", line=dict(color="#E24B4A", width=2)
    ))

if "mq135_ppm" in df.columns:
    fig1.add_trace(go.Scatter(
        x=df["timestamp"], y=df["mq135_ppm"],
        name="MQ-135 (ppm)", line=dict(color="#378ADD", width=1.5),
        yaxis="y2"
    ))

if "temperature_c" in df.columns:
    fig1.add_trace(go.Scatter(
        x=df["timestamp"], y=df["temperature_c"],
        name="Temperature (C)", line=dict(color="#EF9F27", width=1.5),
        yaxis="y2"
    ))

if "humidity_pct" in df.columns:
    fig1.add_trace(go.Scatter(
        x=df["timestamp"], y=df["humidity_pct"],
        name="Humidity (%)", line=dict(color="#1D9E75", width=1.5),
        yaxis="y2"
    ))

# Add AQI threshold lines
fig1.add_hline(y=100, line_dash="dash", line_color="orange",
               annotation_text="Moderate threshold (100)")
fig1.add_hline(y=200, line_dash="dash", line_color="red",
               annotation_text="Unhealthy threshold (200)")

fig1.update_layout(
    title="AQI and Sensor Readings Over Time (Live)",
    xaxis_title="Time",
    yaxis=dict(title="AQI", side="left"),
    yaxis2=dict(title="Sensor Reading", side="right", overlaying="y"),
    height=420,
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02)
)

st.plotly_chart(fig1, use_container_width=True)

st.divider()

# ── Visualization 2: Anomaly Detection ────────────────────────────────────────
st.subheader("Visualization 2 — Anomaly Detection")

col_left, col_right = st.columns(2)

with col_left:
    df_scenarios = query_scenarios()
    if not df_scenarios.empty and "scenario" in df_scenarios.columns:
        color_map = {
            "normal":            "#1D9E75",
            "spike_anomaly":     "#E24B4A",
            "sensor_dropout":    "#EF9F27",
            "out_of_range":      "#D85A30",
            "delayed_timestamp": "#378ADD",
            "duplicate":         "#7F77DD",
        }
        fig2 = px.scatter(
            df_scenarios,
            x="timestamp",
            y="aqi_value",
            color="scenario",
            color_discrete_map=color_map,
            title="AQI Readings by Scenario — Anomalies Highlighted",
            labels={"aqi_value": "AQI Value", "timestamp": "Time"},
        )
        fig2.update_traces(marker=dict(size=6, opacity=0.7))
        fig2.update_layout(height=380)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Scenario data not available yet")

with col_right:
    # Scenario breakdown bar chart
    if not df_scenarios.empty and "scenario" in df_scenarios.columns:
        scenario_counts = df_scenarios["scenario"].value_counts().reset_index()
        scenario_counts.columns = ["scenario", "count"]
        color_map2 = {
            "normal":            "#1D9E75",
            "spike_anomaly":     "#E24B4A",
            "sensor_dropout":    "#EF9F27",
            "out_of_range":      "#D85A30",
            "delayed_timestamp": "#378ADD",
            "duplicate":         "#7F77DD",
        }
        fig_bar = px.bar(
            scenario_counts,
            x="scenario",
            y="count",
            color="scenario",
            color_discrete_map=color_map2,
            title="Record Count by Data Scenario",
            labels={"count": "Number of Records", "scenario": "Scenario Type"},
        )
        fig_bar.update_layout(height=380, showlegend=False)
        st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

# ── Visualization 3: Distribution & Comparison ────────────────────────────────
st.subheader("Visualization 3 — AQI Distribution & Sensor Correlation")

col_v3a, col_v3b = st.columns(2)

with col_v3a:
    if "aqi_value" in df.columns:
        fig3 = px.histogram(
            df,
            x="aqi_value",
            nbins=30,
            title="AQI Value Distribution",
            labels={"aqi_value": "AQI Value", "count": "Frequency"},
            color_discrete_sequence=["#378ADD"],
        )
        # Add vertical threshold lines
        fig3.add_vline(x=100, line_dash="dash", line_color="orange",
                       annotation_text="Moderate")
        fig3.add_vline(x=200, line_dash="dash", line_color="red",
                       annotation_text="Unhealthy")
        fig3.add_vline(x=300, line_dash="dash", line_color="darkred",
                       annotation_text="Hazardous")
        fig3.update_layout(height=380)
        st.plotly_chart(fig3, use_container_width=True)

with col_v3b:
    if "mq135_ppm" in df.columns and "aqi_value" in df.columns:
        fig4 = px.scatter(
            df,
            x="mq135_ppm",
            y="aqi_value",
            color="temperature_c" if "temperature_c" in df.columns else None,
            title="MQ-135 vs AQI — Sensor Correlation",
            labels={
                "mq135_ppm": "MQ-135 Reading (ppm)",
                "aqi_value": "AQI Value",
                "temperature_c": "Temperature (C)"
            },
            color_continuous_scale="RdYlGn_r",
            opacity=0.6,
        )
        fig4.update_layout(height=380)
        st.plotly_chart(fig4, use_container_width=True)

st.divider()

# ── ML Forecast Section ────────────────────────────────────────────────────────
st.subheader("ML Forecast — 30-Minute AQI Prediction")

try:
    from src.output.ml_forecaster import AQIForecaster
    forecaster = AQIForecaster()
    if forecaster.load():
        if len(df) >= 5:
            recent = df.tail(5)
            rolling_avg = recent["aqi_value"].mean() if "aqi_value" in recent.columns else 0
            current_aqi = latest.get("aqi_value", 0) or 0
            mq135_val = latest.get("mq135_ppm", 0) or 0
            mq7_val = latest.get("mq7_ppm", 0) or 0
            temp_val = latest.get("temperature_c", 25) or 25
            hum_val = latest.get("humidity_pct", 50) or 50
            hour_val = datetime.now().hour

            predicted_aqi = forecaster.predict(
                mq135=mq135_val,
                mq7=mq7_val,
                temperature=temp_val,
                humidity=hum_val,
                current_aqi=current_aqi,
                hour_of_day=hour_val,
                rolling_avg=rolling_avg
            )

            col_ml1, col_ml2, col_ml3 = st.columns(3)
            with col_ml1:
                st.metric("Current AQI", f"{current_aqi:.1f}")
            with col_ml2:
                if predicted_aqi:
                    delta = predicted_aqi - current_aqi
                    st.metric("Predicted AQI (30 min)", f"{predicted_aqi:.1f}",
                              delta=f"{delta:+.1f}")
            with col_ml3:
                if predicted_aqi:
                    if predicted_aqi <= 50:
                        st.success("Forecast: Good air quality")
                    elif predicted_aqi <= 100:
                        st.info("Forecast: Moderate air quality")
                    elif predicted_aqi <= 150:
                        st.warning("Forecast: Unhealthy for sensitive groups")
                    elif predicted_aqi <= 200:
                        st.warning("Forecast: Unhealthy — consider ventilation")
                    else:
                        st.error("Forecast: Hazardous — immediate action needed")

            # Feature importance chart
            importance = forecaster.get_feature_importance()
            if importance:
                imp_df = pd.DataFrame(
                    list(importance.items()),
                    columns=["Feature", "Importance"]
                ).sort_values("Importance", ascending=True)
                fig_imp = px.bar(
                    imp_df,
                    x="Importance",
                    y="Feature",
                    orientation="h",
                    title="ML Model — Feature Importance for AQI Forecasting",
                    color="Importance",
                    color_continuous_scale="Reds",
                )
                fig_imp.update_layout(height=350)
                st.plotly_chart(fig_imp, use_container_width=True)
    else:
        st.info("ML model not trained yet. Run: python scripts/train_model.py")
except Exception as e:
    st.info(f"ML model not available: {e}")

st.divider()

# ── Database Stats ─────────────────────────────────────────────────────────────
st.subheader("Database Statistics")
col_a, col_b, col_c, col_d = st.columns(4)

with col_a:
    st.metric("Records in view", len(df))
with col_b:
    if "aqi_value" in df.columns:
        st.metric("Average AQI", f"{df['aqi_value'].mean():.1f}")
with col_c:
    if "aqi_value" in df.columns:
        st.metric("Max AQI", f"{df['aqi_value'].max():.1f}")
with col_d:
    if "aqi_value" in df.columns:
        st.metric("Min AQI", f"{df['aqi_value'].min():.1f}")

if auto_refresh:
    import time
    time.sleep(30)
    st.rerun()
