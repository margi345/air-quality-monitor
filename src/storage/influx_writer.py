import logging
from datetime import datetime
from typing import Optional
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from src.utils.config_loader import get_config

logger = logging.getLogger(__name__)


class InfluxWriter:

    def __init__(self):
        self.config = get_config()
        self.influx_cfg = self.config["influxdb"]
        self.client: Optional[InfluxDBClient] = None
        self.write_api = None
        self.records_written = 0
        self.records_failed = 0

    def connect(self):
        try:
            self.client = InfluxDBClient(
                url=self.influx_cfg["url"],
                token=self.influx_cfg["token"],
                org=self.influx_cfg["org"],
                timeout=self.influx_cfg.get("timeout", 10000),
            )
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            logger.info("InfluxDB connected at %s", self.influx_cfg["url"])
        except Exception as e:
            logger.error("InfluxDB connection failed: %s", e)
            raise

    def write(self, processed: dict) -> bool:
        if self.write_api is None:
            logger.error("InfluxDB not connected — call connect() first")
            return False
        try:
            point = (
                Point("air_quality")
                .tag("device_id", processed.get("device_id", "unknown"))
                .tag("location",  processed.get("location",  "unknown"))
                .tag("aqi_category", processed.get("aqi_category", "unknown"))
                .tag("scenario",  processed.get("scenario",  "normal"))
            )

            field_map = {
                "mq135_ppm":     processed.get("mq135_ppm"),
                "mq7_ppm":       processed.get("mq7_ppm"),
                "temperature_c": processed.get("temperature_c"),
                "humidity_pct":  processed.get("humidity_pct"),
                "aqi_value":     processed.get("aqi_value"),
            }
            for field_name, value in field_map.items():
                if value is not None:
                    point = point.field(field_name, float(value))

            ts = processed.get("timestamp")
            if ts:
                point = point.time(
                    datetime.fromisoformat(ts),
                    write_precision=WritePrecision.SECONDS
                )

            self.write_api.write(
                bucket=self.influx_cfg["bucket"],
                org=self.influx_cfg["org"],
                record=point,
            )
            self.records_written += 1
            return True

        except Exception as e:
            logger.error("InfluxDB write failed: %s", e)
            self.records_failed += 1
            return False

    def close(self):
        if self.client:
            self.client.close()
            logger.info("InfluxDB connection closed. Written: %d Failed: %d",
                self.records_written, self.records_failed)