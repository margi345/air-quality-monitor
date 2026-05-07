# AirGuard — IoT Air Quality Monitor

Real-time indoor air quality monitoring pipeline using ESP32, MQTT, Python, InfluxDB, and Streamlit with ML-based AQI forecasting.

---

## Problem Statement

Indoor air quality is a silent health risk. Poor ventilation, CO gas buildup, and high particulate matter can cause long-term respiratory damage — yet most spaces have no real-time monitoring. AirGuard solves this by continuously monitoring air quality using low-cost sensors, processing the data through a real-time pipeline, storing it in a time-series database, and delivering actionable insights through a live dashboard and a 30-minute AQI forecast.

**Decisions this system enables:**
- Trigger ventilation when AQI exceeds safe thresholds
- Alert occupants when CO levels are dangerous
- Predict air quality deterioration 30 minutes in advance
- Identify patterns of poor air quality by time of day

---

## Architecture

```
+------------------+     MQTT      +-------------------+     Python     +-------------+
|   ESP32 Device   | ------------> |  Mosquitto Broker  | ------------> |  Pipeline   |
|  MQ-135, MQ-7    |  (port 1883)  |    (localhost)     |               |  Processor  |
|  DHT22 Sensors   |               +-------------------+               +------+------+
+------------------+                                                          |
         |                                                                    v
         | (also runs)                                                +-------------+
+------------------+                                                 |  InfluxDB   |
| Python Simulator |                                                 | (localhost  |
| (10,500 records) |                                                 |   :8086)    |
+------------------+                                                 +------+------+
                                                                            |
                                                         +------------------+------------------+
                                                         |                                     |
                                                  +------+------+                  +-----------+----------+
                                                  |  Streamlit  |                  |    ML Forecaster     |
                                                  |  Dashboard  |                  |  (Random Forest)     |
                                                  |   :8501     |                  |  30-min AQI predict  |
                                                  +-------------+                  +----------------------+
```

### Component Justification

| Component | Choice | Justification |
|-----------|--------|---------------|
| Protocol | MQTT | Lightweight pub/sub ideal for IoT sensors with low bandwidth. QoS 1 ensures delivery. Far more efficient than REST for continuous sensor streams. |
| Broker | Mosquitto | Industry-standard open-source MQTT broker. Lightweight, runs locally, easy to configure. |
| Processing | Python | Rich ecosystem (pandas, sklearn). Modular class-based structure matches production standards. |
| Database | InfluxDB v2 | Purpose-built time-series database. Optimized for high-frequency sensor writes. Tag-based indexing enables fast queries by device and location. |
| Visualization | Streamlit | Fast to build, Python-native, supports real-time auto-refresh. Ideal for sensor dashboards. |
| ML | Random Forest | Robust to noisy sensor data. Handles non-linear relationships between gas concentrations and AQI well. |

---

## Project Structure

```
air-quality-monitor/
├── config/
│   └── config.yaml              # Central configuration (MQTT, InfluxDB, ML, sensors)
├── esp32_firmware/
│   └── main.ino                 # ESP32 Arduino firmware
├── scripts/
│   ├── run_pipeline.py          # Start the data pipeline
│   ├── run_simulator.py         # Start the data simulator
│   ├── run_dashboard.py         # Launch Streamlit dashboard
│   ├── train_model.py           # Train the ML forecasting model
│   └── query_influxdb.py        # Sample DB queries
├── src/
│   ├── communication/
│   │   └── mqtt_subscriber.py   # MQTT subscriber with reconnect logic
│   ├── device/
│   │   └── simulator.py         # IoT device simulator
│   ├── processing/
│   │   └── data_processor.py    # Validation, enrichment, deduplication
│   ├── storage/
│   │   └── influx_writer.py     # InfluxDB write client
│   ├── output/
│   │   ├── dashboard.py         # Streamlit dashboard
│   │   └── ml_forecaster.py     # AQI forecasting model
│   └── utils/
│       ├── config_loader.py     # YAML config loader
│       ├── logger_setup.py      # Logging configuration
│       └── aqi_calculator.py    # AQI calculation (US EPA standard)
├── logs/                        # Runtime logs
├── docs/                        # Architecture docs
├── requirements.txt
└── README.md
```

---

## Setup Instructions

### Prerequisites
- Python 3.10+
- Arduino IDE 2.x
- Mosquitto MQTT Broker
- InfluxDB v2
- Git

### 1. Clone the Repository
```bash
git clone https://github.com/margi345/air-quality-monitor.git
cd air-quality-monitor
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Start Mosquitto MQTT Broker
Open CMD as Administrator:
```bash
net start mosquitto
```

### 4. Start InfluxDB
```bash
cd C:\Program Files\InfluxData\influxdb
influxd
```
Then open http://localhost:8086 and log in.

### 5. Configure the Project
Edit `config/config.yaml` and update:
```yaml
influxdb:
  url: "http://localhost:8086"
  token: "YOUR_INFLUXDB_TOKEN"
  org: "your_org_name"
  bucket: "air_quality"
```

### 6. Flash ESP32 Firmware
- Open `esp32_firmware/main.ino` in Arduino IDE
- Update WiFi credentials and MQTT broker IP
- Upload to ESP32 (hold BOOT button when connecting)

---

## Running the Project

Open 3 separate terminals in VS Code:

**Terminal 1 — Start Pipeline:**
```bash
python scripts/run_pipeline.py
```

**Terminal 2 — Start Simulator (or use real ESP32):**
```bash
python scripts/run_simulator.py
```

**Terminal 3 — Launch Dashboard:**
```bash
streamlit run src/output/dashboard.py
```

Open browser at: http://localhost:8501

---

## Train the ML Model

After collecting data, train the forecasting model:
```bash
python scripts/train_model.py
```

The model will be saved to `src/output/models/`.

---

## Database Schema

**Measurement:** `air_quality`

| Type | Name | Description |
|------|------|-------------|
| Tag | `device_id` | Identifies the ESP32 or simulator device |
| Tag | `location` | Physical location (e.g., Lab_Room_204) |
| Tag | `aqi_category` | AQI category (Good, Moderate, Hazardous) |
| Tag | `scenario` | Data scenario (normal, spike, dropout) |
| Field | `mq135_ppm` | MQ-135 air quality sensor reading (ppm) |
| Field | `mq7_ppm` | MQ-7 carbon monoxide reading (ppm) |
| Field | `temperature_c` | DHT22 temperature (Celsius) |
| Field | `humidity_pct` | DHT22 humidity (%) |
| Field | `aqi_value` | Calculated AQI value |
| Timestamp | `_time` | UTC timestamp of reading |

**Schema design rationale:**
Tags are indexed in InfluxDB and used for filtering and grouping. Fields store the actual numeric measurements. This separation enables fast queries by device or location without scanning all field data. The timestamp is mandatory and enables all time-windowed aggregation queries.

### Sample Queries
```bash
python scripts/query_influxdb.py
```

---

## ML Model Details

**Algorithm:** Random Forest Regressor

**Input Features:**

| Feature | Description |
|---------|-------------|
| `mq135_ppm` | Primary air quality indicator |
| `mq7_ppm` | Carbon monoxide level |
| `temperature_c` | Affects gas sensor readings |
| `humidity_pct` | Affects air quality dispersal |
| `current_aqi` | Current AQI baseline |
| `hour_of_day` | Captures daily patterns |
| `rolling_avg_aqi_5` | 5-reading rolling average (highest importance feature) |

**Output:** Predicted AQI value 30 minutes into the future

**How results are used in decision-making:**
- If predicted AQI exceeds 150, the system flags an upcoming unhealthy air quality event
- If predicted AQI exceeds 300, a hazardous air warning is triggered
- The dashboard displays the forecast alongside current readings so occupants can act proactively rather than reactively

---

## Data Validation and Bad Data Handling

| Scenario | Handling Strategy |
|----------|-------------------|
| Null sensor values | Dropped with warning logged |
| Out-of-range values | Rejected based on per-sensor thresholds in config |
| Duplicate messages | Deduplicated using msg_id and 10-second time window |
| Malformed timestamps | Replaced with current UTC time |
| Both gas sensors null | Entire record dropped |
| JSON decode errors | Exception caught and logged, message skipped |

---

## Data Collection Summary

| Attribute | Value |
|-----------|-------|
| Total records | 10,500+ |
| Collection method | Real ESP32 sensors and Python simulator |
| Publish interval | Every 2 seconds |
| Measurable attributes | mq135_ppm, mq7_ppm, temperature_c, humidity_pct, aqi_value |
| Induced scenarios | Spike anomaly, sensor dropout, out-of-range, duplicate, delayed timestamp |

---

## Scaling Considerations

To scale this system to enterprise level, the following changes would be required:

- Replace Mosquitto with AWS IoT Core or HiveMQ to support thousands of concurrent devices
- Replace local InfluxDB with InfluxDB Cloud or TimescaleDB on RDS for managed scalability
- Add Apache Kafka between MQTT and the processing layer for message buffering and replay
- Deploy pipeline components as Docker containers orchestrated with Kubernetes
- Replace Streamlit with Grafana for enterprise-grade dashboards with role-based access
- Implement device authentication using TLS certificates and mutual authentication

---

## Author

Margi,jaydev,khushi — IoT Capstone Project, Spring 2026
