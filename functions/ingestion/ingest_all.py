from .stop_times import load_stop_times, build_stop_features
from .parkingOcupation import parse_parkings, parkings_a_dataframe
from .trafic import parse_trafico, trafico_a_dataframe
from .weather import get_aemet_weather
from .stops import load_stops
import pandas as pd
import numpy as np
import os

aemet_api_key = os.getenv("AEMET_API_KEY")


def get_ingestion_data(municipio: str = "36057") -> dict:
    # Parkings
    parkings = parse_parkings()
    parkings_df = parkings_a_dataframe(parkings)

    # Tráfico
    tramos = parse_trafico()
    trafico_df = trafico_a_dataframe(tramos)

    # Paradas de bus
    stops = load_stops()
    stops_df = pd.DataFrame(stops)

    # Tiempos
    stop_times = load_stop_times()
    stop_times_df = build_stop_features(stops_df, stop_times)

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
        "stops": stops_df,
        "stop_times": stop_times_df
    }

# convierte dataframe a lista de diccionarios, reemplazando NaN por None para compatibilidad con JSON para la funcion de firebase
def df_to_records(data_frame):
    return data_frame.replace({np.nan: None, np.inf: None, -np.inf: None}).to_dict(orient="records")


def get_ingestion_data_json(municipio: str = "36057") -> dict:

    parkings = parse_parkings()
    parkings_df = parkings_a_dataframe(parkings)


    tramos = parse_trafico()
    trafico_df = trafico_a_dataframe(tramos)


    stops = load_stops()
    stops_df = pd.DataFrame(stops)


    stop_times = load_stop_times()
    stop_times_df = build_stop_features(stops_df, stop_times)


    try:
        weather_records = get_aemet_weather(api_key=aemet_api_key, municipio=municipio)

        weather_df = pd.DataFrame([r.__dict__ for r in weather_records])
    except Exception:
        weather_df = pd.DataFrame()

    return {
        "parkings": df_to_records(parkings_df),
        "trafico": df_to_records(trafico_df),
        "weather": df_to_records(weather_df),
        "stops": df_to_records(stops_df),
        "stop_times": df_to_records(stop_times_df),
    }

if __name__ == "__main__":
    get_ingestion_data(aemet_api_key)
