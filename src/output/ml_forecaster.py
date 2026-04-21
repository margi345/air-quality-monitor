import logging
import os
import pickle
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple
from influxdb_client import InfluxDBClient
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error
from src.utils.config_loader import get_config

logger = logging.getLogger(__name__)


class AQIForecaster:

    def __init__(self):
        self.config = get_config()
        self.ml_cfg = self.config["ml"]
        self.influx_cfg = self.config["influxdb"]
        self.model: Optional[RandomForestRegressor] = None
        self.scaler: Optional[StandardScaler] = None
        self.is_trained = False

    def _fetch_training_data(self, hours: int = 6) -> pd.DataFrame:
        client = InfluxDBClient(
            url=self.influx_cfg["url"],
            token=self.influx_cfg["token"],
            org=self.influx_cfg["org"]
        )
        query_api = client.query_api()
        query = f'''
        from(bucket: "air_quality")
          |> range(start: -{hours}h)
          |> filter(fn: (r) => r._measurement == "air_quality")
          |> filter(fn: (r) => r._field == "aqi_value" or
                               r._field == "mq135_ppm" or
                               r._field == "mq7_ppm" or
                               r._field == "temperature_c" or
                               r._field == "humidity_pct")
          |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"])
        '''
        try:
            tables = query_api.query_data_frame(query)
            if isinstance(tables, list):
                df = pd.concat(tables, ignore_index=True)
            else:
                df = tables
            client.close()
            return df
        except Exception as e:
            logger.error("Failed to fetch training data: %s", e)
            client.close()
            return pd.DataFrame()

    def _prepare_features(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        df = df.copy()
        df["_time"] = pd.to_datetime(df["_time"])
        df = df.sort_values("_time").reset_index(drop=True)

        required = ["aqi_value", "mq135_ppm", "mq7_ppm", "temperature_c", "humidity_pct"]
        for col in required:
            if col not in df.columns:
                df[col] = 0.0
        df[required] = df[required].fillna(method="ffill").fillna(0)

        df["hour_of_day"] = df["_time"].dt.hour
        df["rolling_avg_aqi_5"] = df["aqi_value"].rolling(window=5, min_periods=1).mean()

        horizon = self.ml_cfg.get("forecast_horizon_minutes", 30)
        rows_per_minute = 60 // self.config["simulator"]["publish_interval_seconds"]
        shift = horizon * rows_per_minute
        df["target_aqi"] = df["aqi_value"].shift(-shift)
        df = df.dropna(subset=["target_aqi"])

        feature_cols = [
            "mq135_ppm", "mq7_ppm", "temperature_c",
            "humidity_pct", "aqi_value", "hour_of_day", "rolling_avg_aqi_5"
        ]
        X = df[feature_cols].values
        y = df["target_aqi"].values
        return X, y

    def train(self) -> bool:
        logger.info("Fetching training data...")
        hours = self.ml_cfg.get("training_lookback_hours", 6)
        df = self._fetch_training_data(hours)

        if df.empty or len(df) < 50:
            logger.warning("Not enough data to train. Need at least 50 records, got %d", len(df))
            return False

        try:
            X, y = self._prepare_features(df)
            if len(X) < 20:
                logger.warning("Not enough feature rows after preparation: %d", len(X))
                return False

            split = int(len(X) * 0.8)
            X_train, X_test = X[:split], X[split:]
            y_train, y_test = y[:split], y[split:]

            self.scaler = StandardScaler()
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)

            self.model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
            self.model.fit(X_train_scaled, y_train)

            y_pred = self.model.predict(X_test_scaled)
            mae = mean_absolute_error(y_test, y_pred)
            logger.info("Model trained successfully. MAE: %.2f", mae)

            os.makedirs(os.path.dirname(self.ml_cfg["model_path"]), exist_ok=True)
            with open(self.ml_cfg["model_path"], "wb") as f:
                pickle.dump(self.model, f)
            with open(self.ml_cfg["scaler_path"], "wb") as f:
                pickle.dump(self.scaler, f)

            self.is_trained = True
            return True

        except Exception as e:
            logger.error("Training failed: %s", e)
            return False

    def load(self) -> bool:
        try:
            with open(self.ml_cfg["model_path"], "rb") as f:
                self.model = pickle.load(f)
            with open(self.ml_cfg["scaler_path"], "rb") as f:
                self.scaler = pickle.load(f)
            self.is_trained = True
            logger.info("Model loaded successfully")
            return True
        except Exception as e:
            logger.warning("Could not load model: %s", e)
            return False

    def predict(
        self,
        mq135: float,
        mq7: float,
        temperature: float,
        humidity: float,
        current_aqi: float,
        hour_of_day: int,
        rolling_avg: float
    ) -> Optional[float]:
        if not self.is_trained:
            logger.warning("Model not trained yet")
            return None
        try:
            features = np.array([[
                mq135, mq7, temperature,
                humidity, current_aqi,
                hour_of_day, rolling_avg
            ]])
            scaled = self.scaler.transform(features)
            prediction = self.model.predict(scaled)[0]
            return round(float(prediction), 2)
        except Exception as e:
            logger.error("Prediction failed: %s", e)
            return None

    def get_feature_importance(self) -> dict:
        if not self.is_trained:
            return {}
        feature_names = [
            "mq135_ppm", "mq7_ppm", "temperature_c",
            "humidity_pct", "current_aqi",
            "hour_of_day", "rolling_avg_aqi_5"
        ]
        importance = self.model.feature_importances_
        return dict(zip(feature_names, importance))