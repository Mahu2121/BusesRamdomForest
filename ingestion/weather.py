import os
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()
AEMET_API_KEY = os.environ.get("AEMET_API_KEY", "")
import requests

url = "https://opendata.aemet.es/opendata/api/prediccion/ccaa/hoy/gal/"

querystring = {"api_key":AEMET_API_KEY}

headers = {
    'cache-control': "no-cache"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

respuestaJson = response.json()

urlDatos = respuestaJson["datos"]

responseDatos = requests.get(urlDatos, verify=False)

texto = responseDatos.text
texto_limpio = texto.replace('\r', '').strip()
print(texto_limpio)