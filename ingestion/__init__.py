from parkingOcupation import (
    parse_parkings,
    parkings_a_dataframe,
    parkings_a_geodataframe,
    asignar_parkings_a_paradas,
)
from trafic import (
    parse_trafico,
    trafico_a_dataframe,
    trafico_a_geodataframe,
    asignar_trafico_a_paradas,
)
from weather import get_aemet_weather, AEMETIngestion, HourlyWeather
from ingest_all import get_ingestion_data