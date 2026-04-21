import logging
from dataclasses import dataclass
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

MQ135_BREAKPOINTS = [
    (300,  400,   0,  50,  "Good",             "green"),
    (400,  500,  51, 100,  "Moderate",          "yellow"),
    (500,  600, 101, 150,  "Unhealthy for SG",  "orange"),
    (600,  750, 151, 200,  "Unhealthy",         "red"),
    (750,  900, 201, 300,  "Very Unhealthy",    "purple"),
    (900, 1500, 301, 500,  "Hazardous",         "maroon"),
]

CO_BREAKPOINTS = [
    (0,    4.4,   0,  50,  "Good",             "green"),
    (4.5,  9.4,  51, 100,  "Moderate",          "yellow"),
    (9.5, 12.4, 101, 150,  "Unhealthy for SG",  "orange"),
    (12.5, 15.4, 151, 200, "Unhealthy",         "red"),
    (15.5, 30.4, 201, 300, "Very Unhealthy",    "purple"),
    (30.5, 50.4, 301, 500, "Hazardous",         "maroon"),
]


@dataclass
class AQIResult:
    aqi_value: float
    category: str
    color: str
    dominant_pollutant: str
    mq135_sub_index: float
    co_sub_index: float


def _linear_interpolate(concentration: float, breakpoints: list) -> Tuple[float, str, str]:
    for c_low, c_high, i_low, i_high, label, color in breakpoints:
        if c_low <= concentration <= c_high:
            aqi = ((i_high - i_low) / (c_high - c_low)) * (concentration - c_low) + i_low
            return round(aqi, 2), label, color
    if concentration > breakpoints[-1][1]:
        logger.warning("Concentration %.2f exceeds breakpoint table max", concentration)
        return 500.0, "Hazardous", "maroon"
    return 0.0, "Good", "green"


def calculate_aqi(
    mq135_ppm: Optional[float],
    mq7_ppm: Optional[float],
) -> Optional[AQIResult]:
    if mq135_ppm is None and mq7_ppm is None:
        return None

    mq135_sub, mq135_cat, mq135_color = 0.0, "Good", "green"
    co_sub, co_cat, co_color = 0.0, "Good", "green"

    if mq135_ppm is not None and mq135_ppm >= 0:
        mq135_sub, mq135_cat, mq135_color = _linear_interpolate(mq135_ppm, MQ135_BREAKPOINTS)

    if mq7_ppm is not None and mq7_ppm >= 0:
        co_sub, co_cat, co_color = _linear_interpolate(mq7_ppm, CO_BREAKPOINTS)

    if mq135_sub >= co_sub:
        dominant, final_aqi, category, color = "VOC/CO2", mq135_sub, mq135_cat, mq135_color
    else:
        dominant, final_aqi, category, color = "CO", co_sub, co_cat, co_color

    return AQIResult(
        aqi_value=final_aqi,
        category=category,
        color=color,
        dominant_pollutant=dominant,
        mq135_sub_index=mq135_sub,
        co_sub_index=co_sub,
    )


def get_health_recommendation(category: str) -> str:
    recommendations = {
        "Good":             "Air quality is satisfactory. Enjoy outdoor activities.",
        "Moderate":         "Sensitive people should consider reducing outdoor exertion.",
        "Unhealthy for SG": "People with respiratory conditions should limit outdoor exertion.",
        "Unhealthy":        "Everyone may begin to experience health effects.",
        "Very Unhealthy":   "Health alert: everyone may experience serious health effects.",
        "Hazardous":        "Health emergency. Avoid all outdoor exertion immediately.",
    }
    return recommendations.get(category, "No recommendation available.")