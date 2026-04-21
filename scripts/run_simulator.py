import logging
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils.logger_setup import setup_logging
from src.device.simulator import AirQualitySimulator

logger = logging.getLogger(__name__)


def main():
    setup_logging("simulator")
    logger.info("=== AirGuard Simulator Starting ===")
    sim = AirQualitySimulator()
    sim.run()
    logger.info("=== AirGuard Simulator Complete ===")


if __name__ == "__main__":
    main()