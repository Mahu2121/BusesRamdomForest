from parkingOcupation import parse_parkings, parkings_a_dataframe
from trafic import parse_trafico, trafico_a_dataframe
from weather import get_aemet_weather

import pandas as pd


def get_ingestion_data(aemet_api_key: str | None = None, municipio: str = "36057") -> dict:
    # Parkings
    parkings = parse_parkings()
    parkings_df = parkings_a_dataframe(parkings)

    # Tráfico
    tramos = parse_trafico()
    trafico_df = trafico_a_dataframe(tramos)

    # Weather
    try:
        weather_records = get_aemet_weather(api_key=aemet_api_key, municipio=municipio)
        # Convertir a DataFrame
        weather_df = pd.DataFrame([r.__dict__ for r in weather_records])
    except Exception:
        weather_df = pd.DataFrame()

    return {
        "parkings": parkings_df,
        "trafico": trafico_df,
        "weather": weather_df,
    }
