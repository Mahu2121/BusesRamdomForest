import argparse
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, precision_score, recall_score

from training.build_features import FEATURE_COLUMNS, build_features_by_stop


def clasificar_ocupacion(x: float) -> int:
    if x < 0.3:
        return 0
    elif x < 0.7:
        return 1
    else:
        return 2


def build_training_frame(seed: int = 42) -> pd.DataFrame:
    df = build_features_by_stop(seed=seed)

    # Target FUTURO por parada
    df = df.sort_values(["parada_id", "timestamp"], kind="mergesort")
    df["ocupacion_futura"] = df.groupby("parada_id")["aforo_actual"].shift(-1)

    df["target"] = df["ocupacion_futura"].apply(clasificar_ocupacion)

    # Eliminar filas sin futuro (último timestamp de cada parada)
    df = df.dropna(subset=["ocupacion_futura"]).reset_index(drop=True)

    return df


def _print_class_balance(df: pd.DataFrame, label: str):
    vc = df["target"].value_counts(normalize=True).sort_index()
    print(f"\nDistribución de clases ({label}):")
    print(vc)


def preprocess(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """One-hot de parada_id, relleno de NaN y selección de features."""
    # Garantizar que están las columnas esperadas
    missing = [c for c in FEATURE_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"Faltan columnas de features: {missing}")

    # Features: no incluir aforo_actual ni ocupacion_futura
    x_base = df[FEATURE_COLUMNS].copy()

    # timestamp no se usa en el modelo (ya tenemos hora/dia_semana)
    x_base = x_base.drop(columns=["timestamp"])

    x_cat = pd.get_dummies(x_base["parada_id"].astype(str), prefix="parada", dtype=int)
    x_num = x_base.drop(columns=["parada_id"]).apply(pd.to_numeric, errors="coerce")

    x = pd.concat([x_num, x_cat], axis=1).replace([np.inf, -np.inf], np.nan).fillna(0)
    y = df["target"].astype(int)

    return x, y


def temporal_split(df: pd.DataFrame, split_ratio: float = 0.8) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    split = int(len(df) * split_ratio)
    train_df = df.iloc[:split].copy()
    test_df = df.iloc[split:].copy()
    return train_df, test_df


def train_and_evaluate(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    model_out: str,
    random_state: int = 42,
):
    X_train, y_train = preprocess(train_df)
    X_test, y_test = preprocess(test_df)

    # Alinear columnas entre train/test (one-hot puede diferir)
    X_test = X_test.reindex(columns=X_train.columns, fill_value=0)

    model = RandomForestClassifier(
        n_estimators=300,
        max_features="sqrt",
        min_samples_leaf=2,
        class_weight="balanced",
        random_state=random_state,
        n_jobs=-1,
    )

    model.fit(X_train, y_train)
    preds = model.predict(X_test)

    acc = accuracy_score(y_test, preds)
    prec = precision_score(y_test, preds, average="macro", zero_division=0)
    rec = recall_score(y_test, preds, average="macro", zero_division=0)
    f1 = f1_score(y_test, preds, average="macro", zero_division=0)
    cm = confusion_matrix(y_test, preds, labels=[0, 1, 2])

    print(f"Accuracy:  {acc:.4f}")
    print(f"Precision (macro): {prec:.4f}")
    print(f"Recall (macro):    {rec:.4f}")
    print(f"F1-score (macro):  {f1:.4f}")
    print("Confusion matrix (labels 0,1,2):\n", cm)

    joblib.dump(
        {
            "model": model,
            "features": list(X_train.columns),
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "target": "ocupacion_bus_por_parada",
        },
        model_out,
    )
    print(f"Modelo guardado en {model_out}")


def main():
    parser = argparse.ArgumentParser(description="Entrena RandomForest para ocupación futura por parada (0/1/2) con split temporal.")
    parser.add_argument("--out", default="model.pkl", help="Ruta de salida del modelo")
    parser.add_argument("--csv-out", default="", help="Si se indica, guarda el dataset (features+target) a CSV")
    parser.add_argument("--seed", type=int, default=42, help="Semilla")
    parser.add_argument("--split", type=float, default=0.8, help="Ratio de train (temporal)")
    args = parser.parse_args()

    df = build_training_frame(seed=args.seed)

    if args.csv_out:
        import pathlib
        pathlib.Path(args.csv_out).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.csv_out, index=False)
        print(f"Dataset guardado en {args.csv_out}")

    train_df, test_df = temporal_split(df, split_ratio=args.split)

    _print_class_balance(df, "GLOBAL")
    _print_class_balance(train_df, "TRAIN")
    _print_class_balance(test_df, "TEST")

    train_and_evaluate(train_df, test_df, model_out=args.out, random_state=args.seed)


if __name__ == "__main__":
    main()
