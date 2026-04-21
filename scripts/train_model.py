import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import logging
import plotly.express as px
import pandas as pd
from src.utils.logger_setup import setup_logging
from src.output.ml_forecaster import AQIForecaster

logger = logging.getLogger(__name__)


def main():
    setup_logging("train_model")
    logger.info("=== AirGuard ML Training Starting ===")

    forecaster = AQIForecaster()

    logger.info("Training model...")
    success = forecaster.train()

    if not success:
        logger.error("Training failed — make sure you have enough data in InfluxDB")
        logger.info("Run the simulator first: python scripts/run_simulator.py")
        return

    logger.info("Training complete!")

    importance = forecaster.get_feature_importance()
    if importance:
        print("\n=== Feature Importance ===")
        for feature, score in sorted(importance.items(), key=lambda x: x[1], reverse=True):
            bar = "█" * int(score * 50)
            print(f"  {feature:<25} {bar} {score:.4f}")

        df_imp = pd.DataFrame(
            list(importance.items()),
            columns=["Feature", "Importance"]
        ).sort_values("Importance", ascending=True)

        fig = px.bar(
            df_imp,
            x="Importance",
            y="Feature",
            orientation="h",
            title="ML Model — Feature Importance for AQI Forecasting",
            color="Importance",
            color_continuous_scale="Reds",
        )
        fig.update_layout(height=400)
        fig.show()

    print("\n=== Model saved successfully ===")
    print(f"  Model: {forecaster.ml_cfg['model_path']}")
    print(f"  Scaler: {forecaster.ml_cfg['scaler_path']}")
    print("\nNext step: run the Streamlit dashboard to see ML predictions")
    print("  streamlit run scripts/run_dashboard.py")


if __name__ == "__main__":
    main()