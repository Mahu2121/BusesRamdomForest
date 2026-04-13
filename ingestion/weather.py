import os
import urllib3
from dotenv import load_dotenv
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()
AEMET_API_KEY = os.environ.get("AEMET_API_KEY", "")

url = "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/36057"

querystring = {"api_key": AEMET_API_KEY}

headers = {
    'cache-control': "no-cache"
}

response = requests.request("GET", url, headers=headers, params=querystring)

respuestaJson = response.json()

urlDatos = respuestaJson["datos"]

responseDatos = requests.get(urlDatos, verify=False)

texto = responseDatos.text
print(texto)
