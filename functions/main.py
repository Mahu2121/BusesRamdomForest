import json
import firebase_admin
from firebase_admin import db
from firebase_functions import scheduler_fn, https_fn
from datetime import datetime, timezone
from ingestion import ingest_all
import os
import requests


def _get_db_root():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("Falta `DATABASE_URL` (URL de Realtime Database).")

    if not firebase_admin._apps:
        firebase_admin.initialize_app(options={"databaseURL": database_url})

    return db.reference("/")


def _serializar(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"No serializable: {type(obj)}")

def _limpiar_ingestion_antigua(db_root, dias_a_conservar=7):
    ahora = datetime.now(timezone.utc)
    ingestion_ref = db_root.child("ingestion")
    snapshot = ingestion_ref.get()

    if not snapshot:
        return

    for clave in snapshot.keys():
        # Ignorar el nodo "latest"
        if clave == "latest":
            continue
        try:
            fecha = datetime.strptime(clave, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            dias = (ahora - fecha).days
            if dias > dias_a_conservar:
                ingestion_ref.child(clave).delete()
                print(f"Borrado nodo antiguo: {clave}")
        except ValueError:
            pass


def _hacer_ingestion():
    db_root = _get_db_root()
    ahora = datetime.now(timezone.utc)
    day_key = ahora.strftime("%Y-%m-%d")
    payload = ingest_all.get_ingestion_data_json()

    # añadir avisos de tráfico, que no se guardan en DataFrames sino como lista de diccionarios
    try:
        r = requests.get("https://datos.vigo.org/data/trafico/avisos-trafico-es.json", timeout=10)
        payload["avisos_trafico"] = r.json()
    except Exception as e:
        payload["avisos_trafico"] = {"error": str(e)}

    # Limpiar objetos no serializables
    payload_limpio = json.loads(json.dumps(payload, default=_serializar))

    db_root.child("ingestion").child(day_key).set(payload_limpio)
    db_root.child("ingestion").child("latest").set(payload_limpio)

    # Limpiar registros antiguos (conserva los últimos 7 días)
    _limpiar_ingestion_antigua(db_root, dias_a_conservar=7)

@scheduler_fn.on_schedule(schedule="every day 03:00", timezone="Europe/Madrid",region="europe-west1")
def subir_ingestion_a_rtdb(event):
    _hacer_ingestion()

@https_fn.on_request(region="europe-west1")
def trigger_ingestion_manual(req: https_fn.Request):
    _hacer_ingestion()
    return https_fn.Response("OK", status=200)