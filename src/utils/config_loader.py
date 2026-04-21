import os
import yaml
import logging
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "config.yaml"


def load_config(config_path: str = None) -> Dict[str, Any]:
    path = (
        config_path
        or os.environ.get("AIRGUARD_CONFIG_PATH")
        or str(DEFAULT_CONFIG_PATH)
    )
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        try:
            config = yaml.safe_load(f)
            logger.info("Config loaded from: %s", path)
            return config
        except yaml.YAMLError as e:
            logger.error("Failed to parse config YAML: %s", e)
            raise


def get_nested(config: Dict, *keys: str, default: Any = None) -> Any:
    current = config
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


_config: Dict[str, Any] = {}


def get_config() -> Dict[str, Any]:
    global _config
    if not _config:
        _config = load_config()
    return _config