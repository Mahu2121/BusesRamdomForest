from .stop_times import load_stop_times, build_stop_features
from .parkingOcupation import parse_parkings, parkings_a_dataframe, parkings_a_geodataframe, asignar_parkings_a_paradas
from .trafic import parse_trafico, trafico_a_dataframe, trafico_a_geodataframe, asignar_trafico_a_paradas
from .weather import get_aemet_weather
from .stops import load_stops
import pandas as pd
import numpy as np
import os


aemet_api_key = os.getenv("AEMET_API_KEY")

def asignar_y_obtener_stops_enriquecidos(radio_metros_parkings: float = 500,radio_metros_trafico: float = 100):

    stops_df = load_stops()

    required = {"stop_id", "stop_lat", "stop_lon"}
    missing = required - set(stops_df.columns)
    if missing:
        raise ValueError(f"Faltan columnas en stops_df: {sorted(missing)}")

    # Enriquecer con parkings
    parkings_gdf = parkings_a_geodataframe(parse_parkings())
    stops_df = asignar_parkings_a_paradas(
        stops_df=stops_df,
        parkings_gdf=parkings_gdf,
        radio_metros=radio_metros_parkings,
    )

    # Enriquecer con tráfico (sobre el df ya enriquecido con parkings)
    tramos_gdf = trafico_a_geodataframe(parse_trafico())
    stops_df = asignar_trafico_a_paradas(
        stops_df=stops_df,
        tramos_gdf=tramos_gdf,
        radio_metros=radio_metros_trafico,
    )

    return stops_df

def get_ingestion_data(municipio: str = "36057") -> dict:
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
        "weather": weather_df,
        "stops_enriquecidas": df_to_records(asignar_y_obtener_stops_enriquecidos()),
        "stop_times": stop_times_df
    }

# convierte dataframe a lista de diccionarios, reemplazando NaN por None para compatibilidad con JSON para la funcion de firebase
def df_to_records(data_frame):
    return data_frame.replace({np.nan: None, np.inf: None, -np.inf: None}).to_dict(orient="records")


def get_ingestion_data_json(municipio: str = "36057") -> dict:

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
        "weather": df_to_records(weather_df),
        "stops_enriquecidas": df_to_records(asignar_y_obtener_stops_enriquecidos()),
        "stop_times": df_to_records(stop_times_df),
    }

if __name__ == "__main__":
    get_ingestion_data(aemet_api_key)
