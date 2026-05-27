import os
import requests
from dataclasses import dataclass
from typing import Optional


@dataclass
class HourlyWeather:
    fecha: str
    periodo: int
    orto: str
    ocaso: str

    estadoCielo_value: Optional[str] = None
    estadoCielo_descripcion: Optional[str] = None

    temperatura: Optional[float] = None
    sensTermica: Optional[float] = None

    humedadRelativa: Optional[float] = None

    precipitacion: Optional[float] = None
    nieve: Optional[float] = None

    probPrecipitacion: Optional[float] = None
    probNieve: Optional[float] = None
    probTormenta: Optional[float] = None

    viento_direccion: Optional[str] = None
    viento_velocidad: Optional[float] = None
    viento_rachaMax: Optional[float] = None


class AEMETIngestion:

    def __init__(self, raw_json: list):
        self.raw = raw_json
        self.records: list[HourlyWeather] = []


    def parse(self) -> list[HourlyWeather]:
        self.records = []
        for entry in self.raw:
            for dia in entry.get("prediccion", {}).get("dia", []):
                self.parse_dia(dia)
        return self.records


    def parse_dia(self, dia: dict):
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

        prob_prec = self.expand_prob(dia.get("probPrecipitacion", []))
        prob_nieve = self.expand_prob(dia.get("probNieve", []))
        prob_torm = self.expand_prob(dia.get("probTormenta", []))

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
        return {item["periodo"]: item.get("value") for item in items if "periodo" in item and "value" in item}

    @staticmethod
    def map_by_periodo(items: list, value_key="value", extra_key=None) -> dict:
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


def get_aemet_weather(api_key: Optional[str] = None, municipio: str = "36057") -> list:
    if api_key is None:
        api_key = os.environ.get("AEMET_API_KEY")

    if not api_key:
        raise RuntimeError("Falta AEMET_API_KEY")

    url = f"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/{municipio}"
    querystring = {"api_key": api_key} if api_key else {}
    headers = {"cache-control": "no-cache"}

    response = requests.get(url, headers=headers, params=querystring, timeout=15)
    try:
        response.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Error en petición AEMET: {e}") from e

    try:
        respuestaJson = response.json()
    except ValueError as e:
        raise RuntimeError("Respuesta de AEMET no es JSON válido") from e

    if isinstance(respuestaJson, dict) and "datos" in respuestaJson:
        urlDatos = respuestaJson.get("datos")
        if not urlDatos:
            raise RuntimeError("La respuesta de AEMET no contiene una URL válida en 'datos'")

        responseDatos = requests.get(urlDatos, verify=False, timeout=15)
        try:
            responseDatos.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"Error descargando datos AEMET desde {urlDatos}: {e}") from e

        try:
            aemet_response = responseDatos.json()
        except ValueError as e:
            raise RuntimeError("Contenido en 'datos' no es JSON válido") from e

        ingestion = AEMETIngestion(aemet_response)
        print(ingestion)
        return ingestion.parse()
    else:
        raise RuntimeError(f"Respuesta inesperada de AEMET: {respuestaJson}")


if __name__ == "__main__":
    try:
        records = get_aemet_weather()
    except Exception as e:
        print("Error al obtener predicción AEMET:", e)
    else:
        print(f"Registros obtenidos: {len(records)}")
        for r in records[:24]:
            print(r)

