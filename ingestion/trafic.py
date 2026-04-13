import requests

url = "https://datos.vigo.org/data/trafico/treal.geojson"

response = requests.get(url)
response.raise_for_status()

data = response.json()

print(data.keys())
print(data)
