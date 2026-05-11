from pathlib import Path
import pandas as pd


def load_stop_times() -> pd.DataFrame:
    path = Path(__file__).parent.parent / "data" / "stop_times.txt"
    df = pd.read_csv(path)
    df["hour"] = df["arrival_time"].str[:2].astype(int)
    return df

def build_stop_features(stops_df, stop_times_df) -> pd.DataFrame:
    freq = (
        stop_times_df
        .groupby(["stop_id", "hour"])
        .size()
        .reset_index(name="n_expediciones")
    )
    return stops_df.merge(freq, on="stop_id", how="left")