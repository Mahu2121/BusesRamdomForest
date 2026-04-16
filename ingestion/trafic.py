import requests

url = "https://datos.vigo.org/data/trafico/treal.geojson"

response = requests.get(url)
response.raise_for_status()

data = response.json()

print(data.keys())
print(data)


def skeleton(obj, indent=0):
    if isinstance(obj, dict):
        for key in obj:
            print("  " * indent + str(key))
            skeleton(obj[key], indent + 1)
    elif isinstance(obj, list) and obj:
        skeleton(obj[0], indent)

skeleton(data)