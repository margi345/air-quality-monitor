import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from src.utils.config_loader import get_config
from src.utils.aqi_calculator import calculate_aqi, get_health_recommendation

logger = logging.getLogger(__name__)


class DataProcessor:

    def __init__(self):
        self.config = get_config()
        self.proc_cfg = self.config["processing"]
        self.sensor_cfg = self.config["sensors"]
        self.seen_msg_ids: Dict[str, datetime] = {}
        self.records_processed = 0
        self.records_rejected = 0

    def _is_duplicate(self, msg_id: str) -> bool:
        now = datetime.now(timezone.utc)
        window = timedelta(seconds=self.proc_cfg["dedup_window_seconds"])
        if msg_id in self.seen_msg_ids:
            if now - self.seen_msg_ids[msg_id] < window:
                return True
        self.seen_msg_ids[msg_id] = now
        cutoff = now - window * 10
        self.seen_msg_ids = {
            k: v for k, v in self.seen_msg_ids.items() if v > cutoff
        }
        return False

    def _validate_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        try:
            ts = datetime.fromisoformat(timestamp_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            max_drift = timedelta(
                seconds=self.proc_cfg["max_timestamp_drift_seconds"]
            )
            if ts > now + max_drift:
                logger.warning("Timestamp too far in future: %s", timestamp_str)
                return None
            if ts < now - timedelta(hours=24):
                logger.warning("Timestamp too old: %s", timestamp_str)
                return None
            return ts
        except Exception as e:
            logger.error("Invalid timestamp %s: %s", timestamp_str, e)
            return None

    def _validate_sensor_value(
        self,
        value: Any,
        sensor_key: str
    ) -> Optional[float]:
        if value is None:
            logger.debug("Null value for sensor: %s", sensor_key)
            return None
        try:
            value = float(value)
        except (TypeError, ValueError):
            logger.warning("Non-numeric value for %s: %s", sensor_key, value)
            return None
        cfg = self.sensor_cfg.get(sensor_key, {})
        min_val = cfg.get("out_of_range_min", -999999)
        max_val = cfg.get("out_of_range_max", 999999)
        if not (min_val <= value <= max_val):
            logger.warning("Out of range value for %s: %s", sensor_key, value)
            return None
        return value

    def process(self, raw_payload: dict) -> Optional[dict]:
        try:
            msg_id = raw_payload.get("msg_id", "")
            if msg_id and self._is_duplicate(msg_id):
                logger.debug("Duplicate message dropped: %s", msg_id)
                self.records_rejected += 1
                return None

            ts = self._validate_timestamp(raw_payload.get("timestamp", ""))
            if ts is None:
                self.records_rejected += 1
                return None

            mq135 = self._validate_sensor_value(
                raw_payload.get("mq135_ppm"), "mq135"
            )
            mq7 = self._validate_sensor_value(
                raw_payload.get("mq7_ppm"), "mq7"
            )
            temp = self._validate_sensor_value(
                raw_payload.get("temperature_c"), "dht22_temp"
            )
            humidity = self._validate_sensor_value(
                raw_payload.get("humidity_pct"), "dht22_humidity"
            )

            if mq135 is None and mq7 is None:
                logger.warning("Both gas sensors null — dropping record")
                self.records_rejected += 1
                return None

            aqi_result = calculate_aqi(mq135, mq7)
            recommendation = get_health_recommendation(
                aqi_result.category if aqi_result else "Good"
            )

            processed = {
                "device_id":        raw_payload.get("device_id"),
                "location":         raw_payload.get("location"),
                "timestamp":        ts.isoformat(),
                "mq135_ppm":        mq135,
                "mq7_ppm":          mq7,
                "temperature_c":    temp,
                "humidity_pct":     humidity,
                "aqi_value":        aqi_result.aqi_value if aqi_result else None,
                "aqi_category":     aqi_result.category if aqi_result else None,
                "aqi_color":        aqi_result.color if aqi_result else None,
                "dominant_pollutant": aqi_result.dominant_pollutant if aqi_result else None,
                "recommendation":   recommendation,
                "scenario":         raw_payload.get("scenario", "normal"),
                "msg_id":           msg_id,
            }

            self.records_processed += 1
            if self.records_processed % 100 == 0:
                logger.info(
                    "Processed: %d | Rejected: %d | AQI: %.1f (%s)",
                    self.records_processed,
                    self.records_rejected,
                    processed["aqi_value"] or 0,
                    processed["aqi_category"] or "N/A",
                )
            return processed

        except Exception as e:
            logger.error("Unexpected processing error: %s", e)
            self.records_rejected += 1
            return None