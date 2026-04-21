import json
import logging
import time
from typing import Callable, Optional
import paho.mqtt.client as mqtt
from src.utils.config_loader import get_config

logger = logging.getLogger(__name__)


class MQTTSubscriber:

    def __init__(self, on_message_callback: Callable[[dict], None]):
        self.config = get_config()
        self.mqtt_cfg = self.config["mqtt"]
        self.on_message_callback = on_message_callback
        self.client: Optional[mqtt.Client] = None
        self.connected = False
        self.messages_received = 0
        self.messages_failed = 0

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            topic = self.mqtt_cfg["topic_sensor_data"]
            qos = self.mqtt_cfg["qos"]
            client.subscribe(topic, qos=qos)
            logger.info("Subscriber connected and subscribed to: %s", topic)
        else:
            logger.error("Subscriber MQTT connection failed — rc=%d", rc)

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        logger.warning("Subscriber disconnected — rc=%d", rc)

    def _on_message(self, client, userdata, msg):
        self.messages_received += 1
        try:
            raw = msg.payload.decode("utf-8")
            payload = json.loads(raw)
            required_keys = {"device_id", "timestamp"}
            if not required_keys.issubset(payload.keys()):
                logger.warning("Malformed message — skipping: %s", raw[:200])
                self.messages_failed += 1
                return
            logger.debug("Message #%d from device=%s",
                self.messages_received, payload.get("device_id"))
            self.on_message_callback(payload)
        except json.JSONDecodeError as e:
            logger.error("JSON decode error: %s", e)
            self.messages_failed += 1
        except Exception as e:
            logger.error("Unexpected error processing message: %s", e)
            self.messages_failed += 1

    def connect(self):
        self.client = mqtt.Client(
            client_id=self.mqtt_cfg["client_id_subscriber"]
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        retry_delay = self.mqtt_cfg.get("reconnect_delay", 5)
        while not self.connected:
            try:
                self.client.connect(
                    self.mqtt_cfg["broker_host"],
                    self.mqtt_cfg["broker_port"],
                    self.mqtt_cfg["keepalive"],
                )
                self.client.loop_start()
                time.sleep(2)
            except Exception as e:
                logger.error("Connection error: %s — retrying in %ds", e, retry_delay)
                time.sleep(retry_delay)

    def disconnect(self):
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("Subscriber disconnected. Total received: %d failed: %d",
                self.messages_received, self.messages_failed)

    def start(self):
        self.connect()
        logger.info("MQTT Subscriber running — waiting for messages...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Subscriber stopped by user")
            self.disconnect()