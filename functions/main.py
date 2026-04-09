# functions/main.py
import joblib
import numpy as np
import firebase_admin
from firebase_admin import credentials, firestore
from firebase_functions import https_fn, scheduler_fn
from firebase_functions.params import StringParam
import os

# Inicializar Firebase Admin (una sola vez)
firebase_admin.initialize_app()
db = firestore.client()

# Cargar el modelo una vez al arrancar (no en cada petición)
MODEL_PATH = os.path.join(os.path.dirname(__file__), "model.pkl")
model = joblib.load(MODEL_PATH)


# ─────────────────────────────────────────────
# JOB PERIÓDICO: recalcula predicciones
# Se ejecuta cada 15 minutos via Cloud Scheduler
# ─────────────────────────────────────────────
@scheduler_fn.on_schedule(schedule="every 15 minutes")
def recalcular_predicciones(event: scheduler_fn.ScheduledEvent) -> None:
    """
    Lee datos actuales de todas las paradas,
    calcula predicciones y las guarda en Firestore.
    """
    paradas_ref = db.collection("paradas_datos")
    paradas = paradas_ref.stream()

    batch = db.batch()
    count = 0

    for parada in paradas:
        datos = parada.to_dict()
        parada_id = parada.id

        try:
            # Prepara features para el modelo
            features = extraer_features(datos)
            X = np.array([features])

            # Predicción
            prediccion = model.predict(X)[0]
            probabilidad = model.predict_proba(X)[0].max()

            # Guarda resultado en Firestore
            resultado_ref = db.collection("resultados").document(parada_id)
            batch.set(resultado_ref, {
                "prediccion": int(prediccion),
                "probabilidad": float(probabilidad),
                "timestamp": firestore.SERVER_TIMESTAMP,
                "parada_id": parada_id,
            })
            count += 1

        except Exception as e:
            print(f"Error en parada {parada_id}: {e}")

    batch.commit()
    print(f"Predicciones actualizadas: {count} paradas")


# ─────────────────────────────────────────────
# API HTTP: devuelve resultado por ID de parada
# ─────────────────────────────────────────────
@https_fn.on_request()
def get_resultado_parada(req: https_fn.Request) -> https_fn.Response:
    """
    GET /get_resultado_parada?id=PARADA_123
    Devuelve el resultado pre-calculado en Firestore.
    Respuesta típica: ~50-200ms
    """
    # CORS para apps web/móvil
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET",
        "Content-Type": "application/json",
    }

    if req.method == "OPTIONS":
        return https_fn.Response("", status=204, headers=headers)

    parada_id = req.args.get("id")
    if not parada_id:
        return https_fn.Response(
            {"error": "Parámetro 'id' requerido"},
            status=400,
            headers=headers
        )

    try:
        doc = db.collection("resultados").document(parada_id).get()

        if not doc.exists:
            return https_fn.Response(
                {"error": f"No hay datos para la parada {parada_id}"},
                status=404,
                headers=headers
            )

        data = doc.to_dict()
        return https_fn.Response(
            {
                "parada_id": parada_id,
                "prediccion": data["prediccion"],
                "probabilidad": round(data["probabilidad"], 3),
                "ultima_actualizacion": str(data.get("timestamp")),
            },
            status=200,
            headers=headers
        )

    except Exception as e:
        return https_fn.Response(
            {"error": str(e)},
            status=500,
            headers=headers
        )


# ─────────────────────────────────────────────
# Helper: extrae features del documento Firestore
# ─────────────────────────────────────────────
def extraer_features(datos: dict) -> list:
    """Adapta esto a las columnas de tu modelo."""
    return [
        datos.get("temperatura", 0),
        datos.get("lluvia", 0),
        datos.get("hora_del_dia", 12),
        datos.get("dia_semana", 0),
        datos.get("aforo_actual", 0),
        datos.get("retraso_medio", 0),
    ]