import logging
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils.logger_setup import setup_logging
from src.utils.config_loader import get_config
from src.communication.mqtt_subscriber import MQTTSubscriber
from src.processing.data_processor import DataProcessor
from src.storage.influx_writer import InfluxWriter

logger = logging.getLogger(__name__)


def main():
    setup_logging("pipeline")
    config = get_config()

    logger.info("=== AirGuard Pipeline Starting ===")

    influx = InfluxWriter()
    influx.connect()

    processor = DataProcessor()

    def on_message(raw_payload: dict):
        processed = processor.process(raw_payload)
        if processed:
            influx.write(processed)

    subscriber = MQTTSubscriber(on_message_callback=on_message)

    try:
        subscriber.start()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    finally:
        influx.close()
        logger.info("=== AirGuard Pipeline Stopped ===")


if __name__ == "__main__":
    main()