# 🌬️ AirGuard — IoT Air Quality Monitor

> Real-time indoor air quality monitoring pipeline using ESP32, MQTT, Python, InfluxDB, and Streamlit with ML-based AQI forecasting.

---

## 📌 Problem Statement

Indoor air quality is a silent health risk. Poor ventilation, CO gas buildup, and high particulate matter can cause long-term respiratory damage — yet most spaces have no real-time monitoring. **AirGuard** solves this by continuously monitoring air quality using low-cost sensors, processing the data through a real-time pipeline, storing it in a time-series database, and delivering actionable insights through a live dashboard and a 30-minute AQI forecast.

**Decisions this system enables:**
- Trigger ventilation when AQI exceeds safe thresholds
- Alert occupants when CO levels are dangerous
- Predict air quality deterioration 30 minutes in advance
- Identify patterns of poor air quality by time of day

---

## 🏗️ Architecture

```
┌─────────────────┐     MQTT      ┌──────────────────┐     Python     ┌─────────────┐
│   ESP32 Device  │ ────────────► │ Mosquitto Broker  │ ────────────► │  Pipeline   │
│  MQ-135, MQ-7   │  (port 1883)  │   (localhost)     │               │  Processor  │
│  DHT22 Sensors  │               └──────────────────┘               └──────┬──────┘
└─────────────────┘                                                          │
         │                                                                   ▼
         │ (also runs)                                               ┌─────────────┐
┌─────────────────┐                                                  │  InfluxDB   │
│ Python Simulator│                                                  │ (localhost  │
│ (10,500 records)│                                                  │   :8086)    │
└─────────────────┘                                                  └──────┬──────┘
                                                                            │
                                                          ┌─────────────────┴──────────────┐
                                                          │                                │
                                                   ┌──────▼──────┐              ┌──────────▼───────┐
                                                   │  Streamlit  │              │   ML Forecaster  │
                                                   │  Dashboard  │              │ (Random Forest)  │
                                                   │  :8501      │              │ 30-min AQI pred  │
                                                   └─────────────┘              └──────────────────┘
```

### Why Each Component?

| Component | Choice | Justification |
|-----------|--------|---------------|
| **Protocol** | MQTT | Lightweight pub/sub ideal for IoT sensors with low bandwidth. QoS 1 ensures delivery. Far more efficient than REST for continuous sensor streams. |
| **Broker** | Mosquitto | Industry-standard open-source MQTT broker. Lightweight, runs locally, easy to configure. |
| **Processing** | Python | Rich ecosystem (pandas, sklearn). Modular class-based structure matches production standards. |
| **Database** | InfluxDB v2 | Purpose-built time-series DB. Optimized for high-frequency sensor writes. Tag-based indexing enables fast queries by device/location. |
| **Visualization** | Streamlit | Fast to build, Python-native, supports real-time auto-refresh. Ideal for sensor dashboards. |
| **ML** | Random Forest | Robust to noisy sensor data. Handles non-linear relationships between gas concentrations and AQI well. |

---

## 📁 Project Structure

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

## ⚙️ Setup Instructions

### Prerequisites
- Python 3.10+
- Arduino IDE 2.x
- Mosquitto MQTT Broker
- InfluxDB v2
- Git

### 1. Clone the Repository
```bash
git clone https://github.com/krishnaik06/air-quality-monitor.git
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
- Upload to ESP32 (hold BOOT button when prompted)

---

## 🚀 Running the Project

Open **3 separate terminals** in VS Code:

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

## 🤖 Train the ML Model

After collecting data, train the forecasting model:
```bash
python scripts/train_model.py
```

The model will be saved to `src/output/models/`.

---

## 🗄️ Database Schema

**Measurement:** `air_quality`

| Type | Name | Description |
|------|------|-------------|
| **Tag** | `device_id` | Identifies the ESP32 or simulator device |
| **Tag** | `location` | Physical location (e.g., Lab_Room_204) |
| **Tag** | `aqi_category` | AQI category (Good, Moderate, Hazardous...) |
| **Tag** | `scenario` | Data scenario (normal, spike, dropout...) |
| **Field** | `mq135_ppm` | MQ-135 air quality sensor reading (ppm) |
| **Field** | `mq7_ppm` | MQ-7 carbon monoxide reading (ppm) |
| **Field** | `temperature_c` | DHT22 temperature (°C) |
| **Field** | `humidity_pct` | DHT22 humidity (%) |
| **Field** | `aqi_value` | Calculated AQI value |
| **Timestamp** | `_time` | UTC timestamp of reading |

**Why this schema?**
- Tags are indexed → fast filtering by device/location
- Fields store numeric measurements → efficient time-series aggregation
- Timestamp is mandatory in InfluxDB → enables windowed queries

### Sample Queries
```bash
python scripts/query_influxdb.py
```

---

## 🤖 ML Model Details

**Algorithm:** Random Forest Regressor

**Input Features:**
- `mq135_ppm` — primary air quality indicator
- `mq7_ppm` — carbon monoxide level
- `temperature_c` — affects gas sensor readings
- `humidity_pct` — affects air quality dispersal
- `current_aqi` — current baseline
- `hour_of_day` — captures daily patterns
- `rolling_avg_aqi_5` — 5-reading rolling average (most important feature)

**Output:** Predicted AQI value 30 minutes into the future

**How results are used:**
- If predicted AQI > 150 → trigger ventilation alert
- If predicted AQI > 300 → send hazardous air warning
- Dashboard shows forecast alongside current readings for proactive decisions

---

## 📊 Data Validation & Bad Data Handling

The pipeline handles the following bad data scenarios:

| Scenario | How Handled |
|----------|-------------|
| **Null sensor values** | Dropped with warning log |
| **Out-of-range values** | Rejected based on sensor config thresholds |
| **Duplicate messages** | Deduplicated using msg_id + 10s time window |
| **Malformed timestamps** | Replaced with current UTC time |
| **Both gas sensors null** | Record dropped entirely |
| **JSON decode errors** | Caught and logged, message skipped |

---

## 📈 Data Collection

- **Total records:** 10,500+
- **Collection method:** Real ESP32 sensors + Python simulator
- **Publish interval:** Every 2 seconds
- **Induced scenarios:** spike anomaly, sensor dropout, out-of-range, duplicate, delayed timestamp
- **Attributes:** timestamp, mq135_ppm, mq7_ppm, temperature_c, humidity_pct, aqi_value

---

## 🔧 Engineering Standards

- ✅ Modular class-based structure (no single script dump)
- ✅ Central YAML configuration file
- ✅ Structured logging to file and console
- ✅ Error handling with graceful recovery
- ✅ Reusable classes for MQTT, InfluxDB, processing, ML
- ✅ Config-driven execution (change behavior via config, not code)

---

## 📹 Video Demo

[YouTube Link - Coming Soon]

---

## 🏭 Scaling Considerations

To scale this to enterprise level:
- Replace Mosquitto with **AWS IoT Core** or **HiveMQ** for thousands of devices
- Replace local InfluxDB with **InfluxDB Cloud** or **TimescaleDB on RDS**
- Add **Apache Kafka** between MQTT and processing for buffering
- Deploy pipeline as **Docker containers** on Kubernetes
- Add **Grafana** for enterprise-grade dashboards
- Implement **device authentication** with TLS certificates

---

## 👩‍💻 Author

Margi — IoT Capstone Project, Spring 2026
