from datetime import timezone
from typing import Optional

import numpy as np
import pandas as pd


FEATURE_COLUMNS = [
    "parada_id",
    "timestamp",
    "temperatura",
    "lluvia",
    "trafico_nivel",
    "parking_ocupacion",
    "distancia_bus_mas_cercano",
    "tiempo_estimado_llegada",
    "numero_buses_cerca",
    "velocidad_media_linea",
    "tiempo_desde_ultimo_bus",
    "intervalo_entre_buses",
    "hora",
    "dia_semana",
    "es_fin_de_semana",
]


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


def build_features_by_stop(
    *,
    start: Optional[pd.Timestamp] = None,
    periods: int = 7 * 24 * 4,
    freq: str = "15min",
    n_stops: int = 120,
    seed: int = 42,
) -> pd.DataFrame:
    g = _rng(seed)

    if start is None:
        start = pd.Timestamp.now(tz=timezone.utc).floor("min") - pd.Timedelta(days=7)

    timestamps = pd.date_range(start=start, periods=periods, freq=freq, tz=timezone.utc)

    # Paradas simuladas (ids tipo GTFS)
    parada_ids = np.array([f"STOP_{i:04d}" for i in range(1, n_stops + 1)], dtype=object)

    zonas = np.array(["centro", "universidad", "periferia"], dtype=object)
    zona_probs = np.array([0.35, 0.20, 0.45])
    stop_zona = g.choice(zonas, size=n_stops, p=zona_probs)
    zona_factor_map = {"centro": 1.3, "universidad": 1.2, "periferia": 0.8}
    stop_zona_factor = np.vectorize(zona_factor_map.get)(stop_zona).astype(float)

    stop_info = pd.DataFrame({
        "parada_id": parada_ids,
        "zona": stop_zona,
        "zona_factor": stop_zona_factor,
    })

    # Malla parada x timestamp
    idx_stop = np.repeat(parada_ids, len(timestamps))
    idx_time = np.tile(timestamps, len(parada_ids))

    df = pd.DataFrame({
        "parada_id": idx_stop,
        "timestamp": idx_time,
    }).merge(stop_info, on="parada_id", how="left")

    # Variables temporales
    df["hora"] = df["timestamp"].dt.hour.astype(int)
    df["dia_semana"] = df["timestamp"].dt.dayofweek.astype(int)
    df["es_fin_de_semana"] = (df["dia_semana"] >= 5).astype(int)

    # Clima sintético
    df["temperatura"] = 20 + 8 * np.sin((df["hora"] / 24) * 2 * np.pi) + g.normal(0, 2.3, size=len(df))
    # días puntualmente calurosos en la semana
    heatwave = ((df["timestamp"].dt.dayofyear % 7) == 2).astype(int)
    df["temperatura"] += heatwave * g.normal(3.0, 1.0, size=len(df))

    # lluvia como Bernoulli dependiente de día
    day_factor = (df["timestamp"].dt.dayofyear % 5).astype(int)
    day_factor_arr = np.asarray(day_factor)
    p_lluvia = 0.10 + 0.08 * (day_factor_arr == 0).astype(float) + 0.06 * (day_factor_arr == 3).astype(float)
    df["lluvia"] = (g.random(len(df)) < p_lluvia).astype(int)

    # Hora punta: 7–9 y 17–20 (más realista)
    peak = (((df["hora"] >= 7) & (df["hora"] <= 9)) | ((df["hora"] >= 17) & (df["hora"] <= 20))).astype(int)

    # Tráfico 0-1, más alto en punta y laborables
    df["trafico_nivel"] = 0.22 + 0.55 * peak + 0.12 * (1 - df["es_fin_de_semana"]) + 0.08 * df["lluvia"] + g.normal(0, 0.07, size=len(df))
    df["trafico_nivel"] = df["trafico_nivel"].clip(0.0, 1.0)

    # Parking ocupación 0-1 correlacionada con punta y lluvia
    df["parking_ocupacion"] = 0.30 + 0.35 * peak + 0.12 * df["lluvia"] + 0.08 * df["trafico_nivel"] + g.normal(0, 0.10, size=len(df))
    df["parking_ocupacion"] = df["parking_ocupacion"].clip(0.0, 1.0)

    # --- Demanda reforzada (correlación fuerte) ---
    # calor extremo
    calor_extremo = (df["temperatura"] > 28).astype(int)
    trafico_alto = (df["trafico_nivel"] > 0.7).astype(int)

    # base por parada/zona (en escala 0..1 aprox)
    # centro empieza más alto que periferia
    base_zona = 0.30 * df["zona_factor"]

    # finde afecta fuerte sobre todo al centro
    finde_centro = (df["es_fin_de_semana"] * (df["zona"] == "centro").astype(int)).astype(int)

    ruido = g.normal(0, 0.10, size=len(df))

    demanda = (
        base_zona
        + 0.60 * peak
        + 0.25 * df["es_fin_de_semana"]
        + 0.20 * finde_centro
        + 0.30 * df["lluvia"]
        + 0.30 * calor_extremo
        + 0.25 * trafico_alto
        + 0.15 * df["trafico_nivel"]
        + ruido
    )

    # limitar demanda a rango razonable
    demanda = np.clip(demanda, 0.05, 1.80)

    # Features sobre buses cercanos (más buses cuando hay más demanda)
    lam = 0.8 + 4.0 * demanda
    df["numero_buses_cerca"] = np.asarray(g.poisson(lam=lam)).astype(int)

    base_dist = g.gamma(shape=2.0, scale=200.0, size=len(df))
    df["distancia_bus_mas_cercano"] = (base_dist / (1 + df["numero_buses_cerca"])).clip(0.0, 2500.0)

    df["tiempo_estimado_llegada"] = (
        (df["distancia_bus_mas_cercano"] / 220.0) * (1.0 + 1.8 * df["trafico_nivel"]) + g.normal(0, 0.6, size=len(df))
    ).clip(0.0, 75.0)

    df["velocidad_media_linea"] = (30 - 14 * df["trafico_nivel"] - 1.5 * df["lluvia"] + g.normal(0, 2.5, size=len(df))).clip(5.0, 50.0)

    interval_base = 13 - 7 * peak - 2.5 * (df["zona"] == "centro").astype(int) - 1.5 * df["numero_buses_cerca"].clip(0, 4)
    df["intervalo_entre_buses"] = (interval_base + g.normal(0, 2.2, size=len(df))).clip(2.0, 55.0)

    frac = g.random(len(df))
    df["tiempo_desde_ultimo_bus"] = (df["intervalo_entre_buses"] * frac).clip(0.0, 75.0)

    score = (
        -0.6
        + 1.7 * demanda
        + 0.3 * df["lluvia"]
        + 0.2 * calor_extremo
        + 0.2 * trafico_alto
        + g.normal(0, 0.35, size=len(df))
    )
    aforo = 1.0 / (1.0 + np.exp(-score))

    # Mezcla para ensanchar cola alta (más ejemplos de lleno)
    mix = g.random(len(df))
    aforo = np.where(mix < 0.12, np.clip(aforo + 0.25 * g.random(len(df)), 0, 1), aforo)
    aforo = np.where(mix > 0.92, np.clip(aforo - 0.20 * g.random(len(df)), 0, 1), aforo)

    df["aforo_actual"] = pd.Series(aforo).clip(0.0, 1.0)

    # Orden final
    df = df.sort_values(["parada_id", "timestamp"], kind="mergesort").reset_index(drop=True)

    return df

