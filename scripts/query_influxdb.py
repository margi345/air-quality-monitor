import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from influxdb_client import InfluxDBClient
from src.utils.config_loader import get_config

def main():
    config = get_config()
    cfg = config["influxdb"]

    client = InfluxDBClient(
        url=cfg["url"],
        token=cfg["token"],
        org=cfg["org"]
    )
    query_api = client.query_api()

    print("\n========================================")
    print("  AirGuard - InfluxDB Sample Queries")
    print("========================================\n")

    # Query 1 — Latest 10 records
    print("--- Query 1: Latest 10 records ---")
    q1 = '''
    from(bucket: "air_quality")
      |> range(start: -24h)
      |> filter(fn: (r) => r._measurement == "air_quality")
      |> filter(fn: (r) => r._field == "aqi_value")
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: 10)
    '''
    tables = query_api.query(q1)
    for table in tables:
        for record in table.records:
            print(f"  Time: {record.get_time()} | AQI: {record.get_value():.2f}")

    # Query 2 — Average AQI last 1 hour
    print("\n--- Query 2: Average AQI last 1 hour ---")
    q2 = '''
    from(bucket: "air_quality")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "air_quality")
      |> filter(fn: (r) => r._field == "aqi_value")
      |> mean()
    '''
    tables = query_api.query(q2)
    for table in tables:
        for record in table.records:
            print(f"  Average AQI: {record.get_value():.2f}")

    # Query 3 — Max MQ135 reading last 24 hours
    print("\n--- Query 3: Max MQ135 last 24 hours ---")
    q3 = '''
    from(bucket: "air_quality")
      |> range(start: -24h)
      |> filter(fn: (r) => r._measurement == "air_quality")
      |> filter(fn: (r) => r._field == "mq135_ppm")
      |> max()
    '''
    tables = query_api.query(q3)
    for table in tables:
        for record in table.records:
            print(f"  Max MQ135: {record.get_value():.2f} ppm")

    # Query 4 — Count records per AQI category
    print("\n--- Query 4: Records per AQI category ---")
    q4 = '''
    from(bucket: "air_quality")
      |> range(start: -24h)
      |> filter(fn: (r) => r._measurement == "air_quality")
      |> filter(fn: (r) => r._field == "aqi_value")
      |> group(columns: ["aqi_category"])
      |> count()
    '''
    tables = query_api.query(q4)
    for table in tables:
        for record in table.records:
            print(f"  Category: {record.values.get('aqi_category')} | Count: {record.get_value()}")

    # Query 5 — Average temperature last 1 hour
    print("\n--- Query 5: Average temperature last 1 hour ---")
    q5 = '''
    from(bucket: "air_quality")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "air_quality")
      |> filter(fn: (r) => r._field == "temperature_c")
      |> mean()
    '''
    tables = query_api.query(q5)
    for table in tables:
        for record in table.records:
            print(f"  Average Temperature: {record.get_value():.2f} C")

    print("\n========================================")
    print(f"  Total records in DB (approx):")
    q6 = '''
    from(bucket: "air_quality")
      |> range(start: -48h)
      |> filter(fn: (r) => r._measurement == "air_quality")
      |> filter(fn: (r) => r._field == "aqi_value")
      |> count()
    '''
    tables = query_api.query(q6)
    for table in tables:
        for record in table.records:
            print(f"  {record.get_value()} records")
    print("========================================\n")

    client.close()


if __name__ == "__main__":
    main()
    