from pathlib import Path
import pandas as pd

def load_stops() -> pd.DataFrame:
    stops_path = Path(__file__).parent.parent / "data" / "stops.txt"

    df = pd.read_csv(stops_path)
    df["stop_lat"] = pd.to_numeric(df["stop_lat"], errors="coerce")
    df["stop_lon"] = pd.to_numeric(df["stop_lon"], errors="coerce")
    print(df)
    return df

def stops_a_dataframe(stops: dict):
    return pd.DataFrame([p.to_dict() for p in stops.values()]).set_index("id")


stops_loaded = load_stops()