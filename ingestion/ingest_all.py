from ingestion import load_stop_times, build_stop_features
from .parkingOcupation import parse_parkings, parkings_a_dataframe
from .trafic import parse_trafico, trafico_a_dataframe
from .weather import get_aemet_weather
from .stops import load_stops
import pandas as pd


def get_ingestion_data(aemet_api_key: str | None = None, municipio: str = "36057") -> dict:
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

if __name__ == "__main__":
    get_ingestion_data()
