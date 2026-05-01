"""
Unit tests for AirGuard pipeline components.
Run with: pytest tests/test_pipeline.py -v
"""
 
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
 
import pytest
from datetime import datetime, timezone
from src.processing.data_processor import DataProcessor
 
 
# ── Fixtures ──────────────────────────────────────────────────────────────────
 
@pytest.fixture
def processor():
    return DataProcessor()
 
 
@pytest.fixture
def valid_payload():
    return {
        "device_id":     "ESP32_TEST_01",
        "location":      "Lab_Room_204",
        "timestamp":     12345678,
        "mq135_ppm":     450.0,
        "mq7_ppm":       30.0,
        "temperature_c": 25.0,
        "humidity_pct":  55.0,
        "aqi_value":     120.0,
        "scenario":      "normal",
        "msg_id":        "abc123test456def",
    }
 
 
# ── Timestamp Tests ───────────────────────────────────────────────────────────
 
class TestTimestampValidation:
 
    def test_numeric_timestamp_returns_current_time(self, processor):
        """ESP32 sends millis() as integer — should return current UTC time."""
        result = processor._validate_timestamp(12345678)
        assert result is not None
        assert result.tzinfo is not None
 
    def test_string_numeric_timestamp(self, processor):
        """Numeric timestamp as string should also be handled."""
        result = processor._validate_timestamp("9876543")
        assert result is not None
 
    def test_valid_iso_timestamp(self, processor):
        """Valid ISO format timestamp should parse correctly."""
        ts = "2026-05-01T10:00:00+00:00"
        result = processor._validate_timestamp(ts)
        assert result is not None
 
    def test_invalid_timestamp_returns_none(self, processor):
        """Completely invalid timestamp should return None."""
        result = processor._validate_timestamp("not-a-date")
        assert result is None
 
 
# ── Sensor Validation Tests ───────────────────────────────────────────────────
 
class TestSensorValidation:
 
    def test_valid_mq135_value(self, processor):
        """Normal MQ135 reading should pass validation."""
        result = processor._validate_sensor_value(450.0, "mq135")
        assert result == 450.0
 
    def test_valid_mq7_value(self, processor):
        """Normal MQ7 reading should pass validation."""
        result = processor._validate_sensor_value(30.0, "mq7")
        assert result == 30.0
 
    def test_valid_temperature(self, processor):
        """Normal temperature should pass validation."""
        result = processor._validate_sensor_value(25.0, "dht22_temp")
        assert result == 25.0
 
    def test_valid_humidity(self, processor):
        """Normal humidity should pass validation."""
        result = processor._validate_sensor_value(55.0, "dht22_humidity")
        assert result == 55.0
 
    def test_null_value_returns_none(self, processor):
        """Null sensor value should return None."""
        result = processor._validate_sensor_value(None, "mq135")
        assert result is None
 
    def test_non_numeric_value_returns_none(self, processor):
        """Non-numeric sensor value should return None."""
        result = processor._validate_sensor_value("bad_value", "mq135")
        assert result is None
 
    def test_out_of_range_mq135_returns_none(self, processor):
        """MQ135 value above max range should be rejected."""
        result = processor._validate_sensor_value(99999.0, "mq135")
        assert result is None
 
    def test_negative_humidity_returns_none(self, processor):
        """Negative humidity is physically impossible — should be rejected."""
        result = processor._validate_sensor_value(-10.0, "dht22_humidity")
        assert result is None
 
 
# ── Deduplication Tests ───────────────────────────────────────────────────────
 
class TestDeduplication:
 
    def test_first_message_not_duplicate(self, processor):
        """First occurrence of a msg_id should not be flagged as duplicate."""
        result = processor._is_duplicate("unique_msg_001")
        assert result is False
 
    def test_second_message_is_duplicate(self, processor):
        """Second occurrence of same msg_id within window should be flagged."""
        processor._is_duplicate("duplicate_msg_001")
        result = processor._is_duplicate("duplicate_msg_001")
        assert result is True
 
    def test_different_msg_ids_not_duplicates(self, processor):
        """Different msg_ids should never be flagged as duplicates."""
        processor._is_duplicate("msg_001")
        result = processor._is_duplicate("msg_002")
        assert result is False
 
 
# ── Full Processing Pipeline Tests ────────────────────────────────────────────
 
class TestDataProcessor:
 
    def test_valid_payload_processes_successfully(self, processor, valid_payload):
        """A complete valid payload should process without errors."""
        result = processor.process(valid_payload)
        assert result is not None
        assert result["device_id"] == "ESP32_TEST_01"
        assert result["location"] == "Lab_Room_204"
        assert result["mq135_ppm"] == 450.0
        assert result["mq7_ppm"] == 30.0
 
    def test_processed_record_has_aqi_category(self, processor, valid_payload):
        """Processed record should have AQI category enriched."""
        result = processor.process(valid_payload)
        assert result is not None
        assert "aqi_category" in result
        assert result["aqi_category"] is not None
 
    def test_processed_record_has_timestamp(self, processor, valid_payload):
        """Processed record should have a valid ISO timestamp."""
        result = processor.process(valid_payload)
        assert result is not None
        assert "timestamp" in result
        assert result["timestamp"] is not None
 
    def test_missing_gas_sensors_drops_record(self, processor):
        """Record with both gas sensors null should be dropped."""
        payload = {
            "device_id":     "ESP32_TEST_01",
            "location":      "Lab_Room_204",
            "timestamp":     12345678,
            "mq135_ppm":     None,
            "mq7_ppm":       None,
            "temperature_c": 25.0,
            "humidity_pct":  55.0,
            "scenario":      "sensor_dropout",
            "msg_id":        "dropout_test_001",
        }
        result = processor.process(payload)
        assert result is None
 
    def test_duplicate_message_dropped(self, processor, valid_payload):
        """Second message with same msg_id should be dropped."""
        processor.process(valid_payload)
        result = processor.process(valid_payload)
        assert result is None
 
    def test_record_counter_increments(self, processor, valid_payload):
        """Records processed counter should increment on success."""
        initial_count = processor.records_processed
        valid_payload["msg_id"] = "counter_test_unique_001"
        processor.process(valid_payload)
        assert processor.records_processed == initial_count + 1
 
    def test_rejected_counter_increments_on_bad_data(self, processor):
        """Rejected counter should increment when record is dropped."""
        initial_rejected = processor.records_rejected
        bad_payload = {
            "device_id": "TEST",
            "timestamp": 123,
            "mq135_ppm": None,
            "mq7_ppm": None,
            "msg_id": "rejected_test_unique_001",
        }
        processor.process(bad_payload)
        assert processor.records_rejected == initial_rejected + 1
 