import requests

url = "https://datos.vigo.org/data/trafico/parkings-ocupacion.json"

response = requests.request("GET",url)

data = response.json()
print(data)
