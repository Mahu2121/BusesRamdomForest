import firebase_admin

from firebase_admin import db
from firebase_functions import scheduler_fn, https_fn
from datetime import datetime, timezone
from ingestion import ingest_all
import os

def _get_db_root():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("Falta `DATABASE_URL` (URL de Realtime Database).")

    if not firebase_admin._apps:
        firebase_admin.initialize_app(options={"databaseURL": database_url})

    return db.reference("/")

def _hacer_ingestion():
    db_root = _get_db_root()
    ahora = datetime.now(timezone.utc)
    day_key = ahora.strftime("%Y-%m-%d")
    payload = ingest_all.get_ingestion_data_json()
    db_root.child("ingestion").child(day_key).set(payload)
    db_root.child("ingestion").child("latest").set(payload)

@scheduler_fn.on_schedule(schedule="every day 03:00", timezone="Europe/Madrid",region="europe-west1")
def subir_ingestion_a_rtdb(event):
    _hacer_ingestion()

@https_fn.on_request(region="europe-west1")
def trigger_ingestion_manual(req: https_fn.Request):
    _hacer_ingestion()
    return https_fn.Response("OK", status=200)