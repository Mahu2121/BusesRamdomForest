import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import joblib
from build_features import build_features_by_stop
from pathlib import Path

MODEL_PATH = Path(__file__).parent.parent / "model" / "random_forest_aforo.pkl"

FEATURE_COLS = [
    "hora",
    "dia_semana",
    "es_fin_de_semana",
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
]
TARGET_COL = "aforo_actual"


def build_training_frame() -> pd.DataFrame:
    df = build_features_by_stop(
        periods=7 * 24 * 4,
        n_stops=120,
        seed=42,
    )
    return df


def train_model():
    df = build_training_frame()

    X = df[FEATURE_COLS]
    y = df[TARGET_COL]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=12,
        min_samples_leaf=10,
        n_jobs=-1,
        random_state=42,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    print(f"MAE: {mae:.4f}  |  R²: {r2:.4f}")

    importance = pd.Series(model.feature_importances_, index=FEATURE_COLS)
    print("\nTop features:")
    print(importance.sort_values(ascending=False).to_string())

    joblib.dump(model, MODEL_PATH)
    print("\nModelo guardado en model/random_forest_aforo.pkl")
    return model


if __name__ == "__main__":
    train_model()