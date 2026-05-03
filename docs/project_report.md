**# AirGuard Project Report**



**## Use Case**

**Indoor air quality monitoring for health and safety decisions in enclosed spaces such as classrooms, labs, and offices.**



**## Problem Being Solved**

**People spend 90% of their time indoors yet indoor air can be 2-5x more polluted than outdoor air. Carbon monoxide, VOCs, and high CO2 levels cause headaches, fatigue, and long-term respiratory damage. Without continuous monitoring, occupants have no way to know when air quality becomes dangerous.**



**## Database Schema Design**



**### Measurement: air\_quality**



**| Type | Name | Description |**

**|------|------|-------------|**

**| Tag | device\_id | Unique sensor device identifier |**

**| Tag | location | Physical location of sensor |**

**| Tag | aqi\_category | Good/Moderate/Unhealthy |**

**| Field | mq135\_ppm | Air quality reading in ppm |**

**| Field | mq7\_ppm | Carbon monoxide in ppm |**

**| Field | temperature\_c | Temperature in Celsius |**

**| Field | humidity\_pct | Relative humidity percentage |**

**| Field | aqi\_value | Calculated AQI score |**

**| Timestamp | \_time | Auto-indexed by InfluxDB |**



**## Why InfluxDB Schema Fits Our Workload**

**Tags are low-cardinality categorical values used for filtering. Fields are numeric measurements. InfluxDB indexes tags automatically making time-range queries near-instant.**



**## Bad Data Handling**

**1. Null values — sensor dropout detected and flagged**

**2. Out-of-range values — filtered before storage**

**3. Duplicate messages — dedup window of 10 seconds**

**4. Out-of-order timestamps — readings older than 5 minutes rejected**



**## Communication Protocol Justification**

**MQTT was chosen because it is lightweight, supports QoS delivery, and is the industry standard for IoT devices like ESP32.**



**## Scalability Analysis**

**To scale to enterprise level:**

**- Replace Mosquitto with AWS IoT Core**

**- Replace local InfluxDB with InfluxDB Cloud**

**- Add Apache Kafka for high-throughput buffering**

**- Deploy processing as microservices on Kubernetes**

**- Use Grafana for enterprise dashboards**



**## Challenges Faced**

**- ESP32 physical connection instability**

**- InfluxDB organization configuration**

**- MQTT broker external connection setup**

**- Library version compatibility issues**



**## Lessons Learned**

**- IoT pipelines require robust error handling at every layer**

**- Physical hardware introduces unpredictability**

**- Time-series databases differ fundamentally from relational databases**

**- MQTT simplicity makes it ideal for constrained IoT devices**

