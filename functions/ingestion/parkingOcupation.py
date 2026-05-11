import requests
from dataclasses import dataclass, asdict
from datetime import datetime
import pandas as pd
import geopandas as gpd


@dataclass
class Parking:
    id: int
    id_parking: int
    nombre: str
    lat: float
    lon: float
    total_plazas: int
    plazas_libres: int
    ocupacion: int
    fecha_hora: datetime

    # Propiedades

    @property
    def plazas_ocupadas(self) -> int:
        return self.total_plazas - self.plazas_libres

    @property
    def ocupacion_real(self) -> float:
        if self.total_plazas == 0:
            return 0.0
        return round(self.plazas_ocupadas / self.total_plazas * 100, 1)

    @property
    def nivel_ocupacion(self) -> str:
        o = self.ocupacion_real
        if o < 50:   return "bajo"
        if o < 75:   return "medio"
        if o < 90:   return "alto"
        return "saturado"

    @property
    def nivel_ocupacion_num(self) -> int:
        return {"bajo": 0, "medio": 1, "alto": 2, "saturado": 3}[self.nivel_ocupacion]

    def to_dict(self) -> dict:
        d = asdict(self)
        d["fechahora"] = self.fecha_hora.isoformat()
        d["plazas_ocupadas"] = self.plazas_ocupadas
        d["ocupacion_real"] = self.ocupacion_real
        d["nivel_ocupacion"] = self.nivel_ocupacion
        d["nivel_ocupacion_num"] = self.nivel_ocupacion_num
        return d

# Parser

URL_PARKINGS = "https://datos.vigo.org/data/trafico/parkings-ocupacion.json"


def parse_parkings(source=None) -> dict[int, Parking]:
    if source is None:
        response = requests.get(URL_PARKINGS, timeout=10)
        response.raise_for_status()
        data = response.json()
    elif isinstance(source, list):
        data = source
    elif isinstance(source, str):
        import json
        with open(source, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        raise TypeError("source debe ser None, list o str (ruta fichero)")

    parkings: dict[int, Parking] = {}

    for item in data:
        parking = Parking(
            id=int(item["id"]),
            id_parking=int(item["id_parking"]),
            nombre=item["nombre"],
            lat=float(item["lat"]),
            lon=float(item["lon"]),
            total_plazas=int(item["totalplazas"]),
            plazas_libres=int(item["plazaslibres"]),
            ocupacion=int(item["ocupacion"]),
            fecha_hora=datetime.strptime(item["fechahora"], "%Y-%m-%d %H:%M:%S"),
        )
        parkings[parking.id] = parking

    print(parkings)
    return parkings


# Utilidades

def parkings_a_dataframe(parkings: dict):
    return pd.DataFrame([p.to_dict() for p in parkings.values()]).set_index("id")


def parkings_a_geodataframe(parkings: dict):
    df = parkings_a_dataframe(parkings).reset_index()
    return gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["lon"], df["lat"]),
        crs="EPSG:4326",
    ).set_index("id")


def asignar_parkings_a_paradas(stops_df, parkings_gdf, radio_metros: float = 500):

    stops_gdf = gpd.GeoDataFrame(
        stops_df.copy(),
        geometry=gpd.points_from_xy(stops_df["stop_lon"], stops_df["stop_lat"]),
        crs="EPSG:4326",
    ).to_crs("EPSG:25829")

    parkings_utm = parkings_gdf.to_crs("EPSG:25829").reset_index()

    # Buffer de radio alrededor de cada parada
    stops_buf = stops_gdf.copy()
    stops_buf["geometry"] = stops_buf.geometry.buffer(radio_metros)

    joined = gpd.sjoin(stops_buf, parkings_utm[["geometry", "ocupacion_real", "nivel_ocupacion_num"]], how="left",
                       predicate="contains")

    agg = joined.groupby("stop_id").agg(
        parking_ocupacion_media=("ocupacion_real", "mean"),
        parking_nivel_medio=("nivel_ocupacion_num", "mean"),
        parking_n=("ocupacion_real", "count"),
        parking_saturados=("ocupacion_real", lambda x: (x >= 90).sum()),
    ).reset_index()

    return stops_df.merge(agg, on="stop_id", how="left")


if __name__ == "__main__":
    parkings = parse_parkings()
