import os
import urllib3
from dotenv import load_dotenv
import requests
from dataclasses import dataclass
from typing import Optional


@dataclass
class HourlyWeather:
    """Registro meteorológico por hora para un día concreto."""
    fecha: str
    periodo: int  # hora (0-23)
    orto: str
    ocaso: str

    # Estado del cielo
    estadoCielo_value: Optional[str] = None
    estadoCielo_descripcion: Optional[str] = None

    # Temperaturas
    temperatura: Optional[float] = None
    sensTermica: Optional[float] = None

    # Humedad
    humedadRelativa: Optional[float] = None

    # Precipitación y nieve
    precipitacion: Optional[float] = None
    nieve: Optional[float] = None

    probPrecipitacion: Optional[float] = None
    probNieve: Optional[float] = None
    probTormenta: Optional[float] = None

    # Viento
    viento_direccion: Optional[str] = None
    viento_velocidad: Optional[float] = None
    viento_rachaMax: Optional[float] = None


class AEMETIngestion:
    """
    Clase de ingesta para el JSON horario de AEMET.
    Transforma la respuesta de la API en una lista de HourlyWeather,
    uno por cada hora disponible en cada día de la predicción.
    """

    def __init__(self, raw_json: list):
        self.raw = raw_json
        self.records: list[HourlyWeather] = []


    def parse(self) -> list[HourlyWeather]:
        """Parsea el JSON completo y devuelve los registros horarios."""
        self.records = []
        for entry in self.raw:
            for dia in entry.get("prediccion", {}).get("dia", []):
                self.parse_dia(dia)
        return self.records


    def parse_dia(self, dia: dict):
        """Construye un HourlyWeather por cada hora dentro de un día."""
        fecha = dia.get("fecha", "")
        orto = dia.get("orto", "")
        ocaso = dia.get("ocaso", "")

        cielo_map = self.map_by_periodo(dia.get("estadoCielo", []), value_key="value", extra_key="descripcion")
        temp_map = self.simple_map(dia.get("temperatura", []))
        sens_map = self.simple_map(dia.get("sensTermica", []))
        hum_map = self.simple_map(dia.get("humedadRelativa", []))
        prec_map = self.simple_map(dia.get("precipitacion", []))
        nieve_map = self.simple_map(dia.get("nieve", []))
        viento_map = self.parse_viento(dia.get("vientoAndRachaMax", []))

        # Probabilidades: cubren rangos ("0814"), las expandimos a cada hora
        prob_prec = self.expand_prob(dia.get("probPrecipitacion", []))
        prob_nieve = self.expand_prob(dia.get("probNieve", []))
        prob_torm = self.expand_prob(dia.get("probTormenta", []))

        # Unión de todos los periodos horarios presentes
        all_periodos = set()
        for m in [cielo_map, temp_map, sens_map, hum_map, prec_map, nieve_map, viento_map]:
            all_periodos.update(m.keys())

        for periodo in sorted(all_periodos):
            cielo = cielo_map.get(periodo, {})
            viento = viento_map.get(periodo, {})

            record = HourlyWeather(
                fecha=fecha,
                periodo=int(periodo),
                orto=orto,
                ocaso=ocaso,
                estadoCielo_value=cielo.get("value"),
                estadoCielo_descripcion=cielo.get("descripcion"),
                temperatura=self._to_float(temp_map.get(periodo)),
                sensTermica=self._to_float(sens_map.get(periodo)),
                humedadRelativa=self._to_float(hum_map.get(periodo)),
                precipitacion=self._to_float(prec_map.get(periodo)),
                nieve=self._to_float(nieve_map.get(periodo)),
                probPrecipitacion=self._to_float(prob_prec.get(periodo)),
                probNieve=self._to_float(prob_nieve.get(periodo)),
                probTormenta=self._to_float(prob_torm.get(periodo)),
                viento_direccion=viento.get("direccion"),
                viento_velocidad=self._to_float(viento.get("velocidad")),
                viento_rachaMax=self._to_float(viento.get("rachaMax")),
            )
            self.records.append(record)


    @staticmethod
    def simple_map(items: list) -> dict:
        """{'periodo': value_str} para arrays simples {value, periodo}."""
        return {item["periodo"]: item.get("value") for item in items if "periodo" in item and "value" in item}

    @staticmethod
    def map_by_periodo(items: list, value_key="value", extra_key=None) -> dict:
        """{'periodo': {value_key: ..., extra_key: ...}} para arrays con campos extra."""
        result = {}
        for item in items:
            p = item.get("periodo")
            if p is None:
                continue
            result[p] = {value_key: item.get(value_key)}
            if extra_key:
                result[p][extra_key] = item.get(extra_key)
        return result

    @staticmethod
    def parse_viento(items: list) -> dict:
        """
        vientoAndRachaMax mezcla dos tipos de registros por periodo:
          - {direccion: [...], velocidad: [...], periodo: "HH"}  → velocidad media
          - {value: "XX", periodo: "HH"}                         → racha máxima
        """
        viento: dict = {}
        for item in items:
            p = item.get("periodo")
            if p is None:
                continue
            if p not in viento:
                viento[p] = {}
            if "direccion" in item:
                viento[p]["direccion"] = item["direccion"][0] if item["direccion"] else None
                viento[p]["velocidad"] = item["velocidad"][0] if item.get("velocidad") else None
            elif "value" in item:
                viento[p]["rachaMax"] = item["value"]
        return viento

    @staticmethod
    def expand_prob(items: list) -> dict:
        """
        Expande registros de probabilidad con rango ("0814") a cada hora individual.
        También admite periodos de un solo bloque ("2002" → horas 20, 21, 22, 23, 00, 01).
        """
        expanded = {}
        for item in items:
            periodo = item.get("periodo", "")
            value = item.get("value")
            if len(periodo) == 4:
                start = int(periodo[:2])
                end = int(periodo[2:])
                if start < end:
                    horas = range(start, end)
                else:
                    horas = list(range(start, 24)) + list(range(0, end))
                for h in horas:
                    expanded[str(h).zfill(2)] = value
            else:
                expanded[periodo] = value
        return expanded

    @staticmethod
    def _to_float(value) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


if __name__ == "__main__":

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

    aemet_response = responseDatos.json()

    print(aemet_response)

    ingestion = AEMETIngestion(aemet_response)
    records = ingestion.parse()

    for r in records[4:28]:
        print(r)