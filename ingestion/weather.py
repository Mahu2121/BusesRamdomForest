import os
from dotenv import load_dotenv
from aemet import Aemet

# Cargar variables del archivo .env
load_dotenv()
AEMET_API_KEY = os.environ.get("AEMET_API_KEY", "")
aemet_client = Aemet(api_key=AEMET_API_KEY)

# Predicción normalizada para Galicia (ccaa='17') para hoy
prediccion = aemet_client.get_prediccion_normalizada(
    ambito='ccaa',
    dia='hoy',
    ccaa='17'  # Código de Galicia
)

print("Predicción AEMET para Galicia")
print(prediccion)