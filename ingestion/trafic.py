import geopandas as gpd
import requests
import json
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Optional
from pathlib import Path
import pandas as pd
from shapely.geometry import LineString

URL_TRAFICO = "https://datos.vigo.org/data/trafico/treal.geojson"

STYLE_A_ESTADO = {
    "#FLUIDO": "Fluido",
    "#DENSO": "Denso",
    "#MUYDENSO": "Muy denso",
    "#LENTO": "Lento",
    "#SINDATOS": "Sin datos",
}

ESTADO_A_NIVEL = {
    "Fluido": 1,
    "Normal": 2,
    "Denso": 3,
    "Lento": 3,
    "Muy denso": 4,
    "Sin datos": 0,
}


@dataclass
class TramoTrafico:
    id_tramo: int
    gid: int
    nombre_tramo: str
    estado: str
    vel_media: float
    segundos: int
    tiempo: str
    longitud: int
    vehiculos: Optional[int]
    nodo_origen: str
    nodo_destino: int
    actualizacion: datetime
    style: str
    coordinates: list = field(default_factory=list)

    # Propiedades

    @property
    def nivel_congestion(self) -> int:
        """0 (sin datos) → 4 (muy denso)"""
        return ESTADO_A_NIVEL.get(self.estado, 0)

    @property
    def vel_libre_ms(self) -> float:
        """Velocidad real del tramo en m/s"""
        return round(self.longitud / self.segundos, 2) if self.segundos else 0.0

    @property
    def ratio_congestion(self) -> float:
        """
        Cuánto más lento va el tramo respecto a su velocidad media.
        1.0 = fluido, >1.5 = congestionado.
        Buen feature continuo para el modelo.
        """
        if self.vel_media == 0:
            return 0.0
        tiempo_libre = (self.longitud / 1000) / self.vel_media * 3600
        return round(self.segundos / tiempo_libre, 2) if tiempo_libre else 0.0

    @property
    def bbox(self) -> dict:
        if not self.coordinates:
            return {}
        lons = [c[0] for c in self.coordinates]
        lats = [c[1] for c in self.coordinates]
        return {"min_lon": min(lons), "max_lon": max(lons),
                "min_lat": min(lats), "max_lat": max(lats)}

    def to_dict(self) -> dict:
        d = asdict(self)
        d["actualizacion"] = self.actualizacion.isoformat()
        d["nivel_congestion"] = self.nivel_congestion
        d["vel_libre_ms"] = self.vel_libre_ms
        d["ratio_congestion"] = self.ratio_congestion
        return d

# Parsers

def _parse_fecha(raw: str) -> datetime:
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    return datetime.min


def parse_trafico(source=None) -> dict[int, TramoTrafico]:
    if source is None:
        response = requests.get(URL_TRAFICO, timeout=10)
        response.raise_for_status()
        data = response.json()
    elif isinstance(source, dict):
        data = source
    elif isinstance(source, (str, Path)):
        with open(source, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        raise TypeError("source debe ser None, dict o ruta (str/Path)")

    tramos: dict[int, TramoTrafico] = {}

    for feature in data.get("features", []):
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})

        coords = geom.get("coordinates", []) if geom.get("type") == "LineString" else []

        estado = (props.get("estado") or "").strip()
        if not estado:
            estado = STYLE_A_ESTADO.get(props.get("style", ""), "Sin datos")

        tramo = TramoTrafico(
            id_tramo=int(props["id_tramo"]),
            gid=int(props.get("gid", 0)),
            nombre_tramo=props.get("nombre_tramo", ""),
            estado=estado,
            vel_media=float(props.get("vel_media") or 0),
            segundos=int(props.get("segundos") or 0),
            tiempo=props.get("tiempo", ""),
            longitud=int(props.get("longitud") or 0),
            vehiculos=int(props["vehiculos"]) if props.get("vehiculos") is not None else None,
            nodo_origen=str(props.get("nodo_origen", 0)),
            nodo_destino=int(props.get("nodo_destino", 0)),
            actualizacion=_parse_fecha(props.get("actualizacion", "")),
            style=props.get("style", ""),
            coordinates=coords,
        )
        tramos[tramo.id_tramo] = tramo

    return tramos


def trafico_a_dataframe(tramos: dict):
    rows = [t.to_dict() for t in tramos.values()]
    for r in rows:
        r.pop("coordinates")
    return pd.DataFrame(rows).set_index("id_tramo")


def trafico_a_geodataframe(tramos: dict):
    rows = []
    for t in tramos.values():
        d = t.to_dict()
        coords = d.pop("coordinates")
        d["geometry"] = LineString(coords) if len(coords) >= 2 else None
        rows.append(d)

    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326").set_index("id_tramo")


def asignar_trafico_a_paradas(stops_df, tramos_gdf, radio_metros: float = 100):
    """
       Para cada parada GTFS asigna el tramo de tráfico más cercano.

       Args:
           stops_df:     DataFrame con columnas [stop_id, stop_lat, stop_lon]
           tramos_gdf:   GeoDataFrame de trafico_a_geodataframe()
           radio_metros: distancia máxima en metros (default 100m)

       Returns:
           stops_df enriquecido con:
             id_tramo_cercano, nombre_tramo, estado,
             vel_media, nivel_congestion, ratio_congestion, dist_m
       """
    stops_gdf = gpd.GeoDataFrame(
        stops_df.copy(),
        geometry=gpd.points_from_xy(stops_df["stop_lon"], stops_df["stop_lat"]),
        crs="EPSG:4326",
    ).to_crs("EPSG:25829")

    cols = ["geometry", "nombre_tramo", "estado",
            "vel_media", "nivel_congestion", "ratio_congestion"]
    tramos_utm = tramos_gdf[cols].to_crs("EPSG:25829")

    joined = gpd.sjoin_nearest(
        stops_gdf,
        tramos_utm,
        how="left",
        max_distance=radio_metros,
        distance_col="dist_m",
    ).rename(columns={"index_right": "id_tramo_cercano"})

    return joined.drop(columns=["geometry"])


if __name__ == "__main__":

    tramos = parse_trafico()

    print(f"{'id':>4} {'estado':<12} {'vel km/h':>8} {'long':>7} {'ratio':>6}  nombre")
    print("-" * 72)
    for tid, t in sorted(tramos.items()):
        print(f"{tid:>4} {t.estado:<12} {t.vel_media:>7.0f}k {t.longitud:>6}m "
              f"{t.ratio_congestion:>5.2f}x  {t.nombre_tramo[:35]}         {t.coordinates} ")
