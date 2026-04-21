import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone, timedelta
import paho.mqtt.client as mqtt
from src.utils.config_loader import get_config
from src.utils.logger_setup import setup_logging

logger = logging.getLogger(__name__)


class AirQualitySimulator:

    def __init__(self):
        self.config = get_config()
        self.sim_cfg = self.config["simulator"]
        self.mqtt_cfg = self.config["mqtt"]
        self.device_id = self.sim_cfg["device_id"]
        self.location = self.sim_cfg["location"]
        self.client = None
        self.connected = False
        self.records_sent = 0
        self._base_mq135 = 420.0
        self._base_mq7 = 5.0
        self._base_temp = 22.0
        self._base_humidity = 55.0
        self._drift_direction = 1

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            logger.info("Simulator connected to MQTT broker")
        else:
            logger.error("Simulator MQTT connection failed — rc=%d", rc)

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        logger.warning("Simulator disconnected — rc=%d", rc)

    def connect(self):
        self.client = mqtt.Client(
            client_id=f"{self.mqtt_cfg['client_id_publisher']}_{uuid.uuid4().hex[:6]}"
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        retry_delay = self.mqtt_cfg.get("reconnect_delay", 5)
        while not self.connected:
            try:
                logger.info("Connecting to MQTT broker %s:%d",
                    self.mqtt_cfg["broker_host"],
                    self.mqtt_cfg["broker_port"])
                self.client.connect(
                    self.mqtt_cfg["broker_host"],
                    self.mqtt_cfg["broker_port"],
                    self.mqtt_cfg["keepalive"],
                )
                self.client.loop_start()
                time.sleep(2)
            except Exception as e:
                logger.error("MQTT connect error: %s — retrying in %ds", e, retry_delay)
                time.sleep(retry_delay)

    def _generate_normal_reading(self) -> dict:
        if self.records_sent % 100 == 0:
            self._drift_direction *= -1
            self._base_mq135 += self._drift_direction * random.uniform(10, 30)
            self._base_mq7 += self._drift_direction * random.uniform(0.5, 2)
            self._base_mq135 = max(320, min(700, self._base_mq135))
            self._base_mq7 = max(2, min(40, self._base_mq7))
        return {
            "mq135_ppm":     round(random.gauss(self._base_mq135, 15), 2),
            "mq7_ppm":       round(random.gauss(self._base_mq7, 1.5), 2),
            "temperature_c": round(random.gauss(self._base_temp, 0.5), 2),
            "humidity_pct":  round(random.gauss(self._base_humidity, 2), 2),
        }

    def _apply_failure_scenario(self, reading: dict):
        scenario = "normal"
        r = random.random()
        if r < self.sim_cfg["anomaly_probability"]:
            reading["mq135_ppm"] = round(reading["mq135_ppm"] * random.uniform(2.0, 3.5), 2)
            reading["mq7_ppm"]   = round(reading["mq7_ppm"] * random.uniform(1.5, 4.0), 2)
            scenario = "spike_anomaly"
        elif r < self.sim_cfg["anomaly_probability"] + self.sim_cfg["dropout_probability"]:
            if random.random() < 0.5:
                reading["mq135_ppm"] = None
            else:
                reading["mq7_ppm"] = None
            scenario = "sensor_dropout"
        elif r < (self.sim_cfg["anomaly_probability"]
                + self.sim_cfg["dropout_probability"]
                + self.sim_cfg["out_of_range_probability"]):
            reading["mq135_ppm"] = round(random.uniform(5000, 9999), 2)
            scenario = "out_of_range"
        return reading, scenario

    def _build_payload(self) -> dict:
        reading = self._generate_normal_reading()
        reading, scenario = self._apply_failure_scenario(reading)
        now = datetime.now(timezone.utc)
        if random.random() < self.sim_cfg["delayed_probability"]:
            now = now - timedelta(seconds=random.randint(30, 300))
            scenario = scenario if scenario != "normal" else "delayed_timestamp"
        return {
            "device_id":     self.device_id,
            "location":      self.location,
            "timestamp":     now.isoformat(),
            "mq135_ppm":     reading.get("mq135_ppm"),
            "mq7_ppm":       reading.get("mq7_ppm"),
            "temperature_c": reading.get("temperature_c"),
            "humidity_pct":  reading.get("humidity_pct"),
            "scenario":      scenario,
            "msg_id":        uuid.uuid4().hex,
        }

    def run(self):
        self.connect()
        total    = self.sim_cfg["total_records"]
        interval = self.sim_cfg["publish_interval_seconds"]
        topic    = self.mqtt_cfg["topic_sensor_data"]
        qos      = self.mqtt_cfg["qos"]
        logger.info("Simulator starting — target records: %d", total)
        last_payload = None
        while self.records_sent < total:
            if not self.connected:
                logger.warning("Not connected — waiting...")
                time.sleep(5)
                continue
            payload = self._build_payload()
            if (last_payload is not None
                    and random.random() < self.sim_cfg["duplicate_probability"]):
                dup = last_payload.copy()
                dup["scenario"] = "duplicate"
                self.client.publish(topic, json.dumps(dup), qos=qos)
            self.client.publish(topic, json.dumps(payload), qos=qos)
            last_payload = payload
            self.records_sent += 1
            if self.records_sent % 500 == 0:
                logger.info("Records published: %d / %d | scenario=%s",
                    self.records_sent, total, payload["scenario"])
            time.sleep(interval)
        logger.info("Simulation complete. Total records: %d", self.records_sent)
        self.client.loop_stop()
        self.client.disconnect()


if __name__ == "__main__":
    setup_logging("simulator")
    sim = AirQualitySimulator()
    sim.run()