import requests
from dataclasses import dataclass, asdict
from datetime import datetime

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
    fechahora: datetime

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
        d["fechahora"] = self.fechahora.isoformat()
        d["plazas_ocupadas"] = self.plazas_ocupadas
        d["ocupacion_real"] = self.ocupacion_real
        d["nivel_ocupacion"] = self.nivel_ocupacion
        d["nivel_ocupacion_num"] = self.nivel_ocupacion_num
        return d


# Parser

URL_PARKINGS = "https://datos.vigo.org/data/trafico/parkings-ocupacion.json"


def parse_parkings(source=None) -> dict[int, Parking]:
    """
    Parsea la API de parkings y devuelve {id: Parking}.

    Args:
        source: None  → descarga en tiempo real desde la API
                list  → lista de dicts ya cargada (p.ej. response.json())
                str   → ruta a fichero JSON local
    """
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
            fechahora=datetime.strptime(item["fechahora"], "%Y-%m-%d %H:%M:%S"),
        )
        parkings[parking.id] = parking

    return parkings


# Utilidades

def parkings_a_dataframe(parkings: dict):
    """Convierte a DataFrame de pandas."""
    import pandas as pd
    return pd.DataFrame([p.to_dict() for p in parkings.values()]).set_index("id")


def parkings_a_geodataframe(parkings: dict):
    """Convierte a GeoDataFrame con geometría Point (EPSG:4326)."""
    import geopandas as gpd
    df = parkings_a_dataframe(parkings).reset_index()
    return gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["lon"], df["lat"]),
        crs="EPSG:4326",
    ).set_index("id")


def asignar_parkings_a_paradas(stops_df, parkings_gdf, radio_metros: float = 500):
    """
    Para cada parada GTFS, calcula la ocupación media de los parkings
    que estén dentro de `radio_metros`.

    Args:
        stops_df:     DataFrame con columnas [stop_id, stop_lat, stop_lon]
        parkings_gdf: GeoDataFrame resultado de parkings_a_geodataframe()
        radio_metros: radio de búsqueda en metros (default 500m)

    Returns:
        stops_df enriquecido con:
          - parking_ocupacion_media   → % medio de ocupación en el radio
          - parking_nivel_medio       → nivel_ocupacion_num medio
          - parking_n                 → nº de parkings en el radio
          - parking_saturados         → nº de parkings saturados (>90%)
    """

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

    print(f"{'id':<4} {'nombre':<30} {'libre':>6} {'total':>6} {'ocup%':>6}  {'nivel'}")
    print("-" * 65)
    for pid, p in sorted(parkings.items()):
        print(
            f"{pid:<4} {p.nombre:<30} {p.plazas_libres:>6} {p.total_plazas:>6} {p.ocupacion_real:>5.1f}%  {p.nivel_ocupacion}")

    print(f"\n── Actualizado: {list(parkings.values())[0].fechahora}")

    # Totales
    total_plazas = sum(p.total_plazas for p in parkings.values())
    total_libres = sum(p.plazas_libres for p in parkings.values())
    ocup_global = round((total_plazas - total_libres) / total_plazas * 100, 1)
    print(f"\n── Global: {total_plazas} plazas | {total_libres} libres | {ocup_global}% ocupado")
