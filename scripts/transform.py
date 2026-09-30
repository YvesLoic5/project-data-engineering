"""
Module de transformation : aplati les donnees horaires brutes issues
d'Open-Meteo en une liste de documents exploitables, puis applique
plusieurs transformations de nettoyage :

1. Suppression des doublons (couple ville + horodatage).
2. Gestion des valeurs manquantes (lignes incompletes ecartees).
3. Conversion de types (temperatures/pluie/vent en float, code en int).
4. Normalisation des dates au format ISO 8601 UTC-naive.
5. Creation d'une nouvelle colonne 'condition' a partir du code meteo WMO.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Mapping simplifie des codes meteo WMO vers une description lisible
WEATHER_CODE_MAP = {
    0: "Ciel degage",
    1: "Principalement degage",
    2: "Partiellement nuageux",
    3: "Couvert",
    45: "Brouillard",
    48: "Brouillard givrant",
    51: "Bruine legere",
    53: "Bruine moderee",
    55: "Bruine dense",
    61: "Pluie legere",
    63: "Pluie moderee",
    65: "Pluie forte",
    71: "Neige legere",
    80: "Averses legeres",
    81: "Averses moderees",
    82: "Averses violentes",
    95: "Orage",
}

REQUIRED_FIELDS = ["ville", "timestamp", "temperature_c", "humidite_pct"]


def _flatten(raw: dict) -> list[dict]:
    """Transforme le JSON brut (colonnes paralleles) en une liste de dicts (une ligne par heure)."""
    documents = []
    for entree in raw["villes"]:
        ville = entree["ville"]
        hourly = entree.get("hourly", {})
        temps = hourly.get("time", [])
        temperatures = hourly.get("temperature_2m", [])
        humidites = hourly.get("relative_humidity_2m", [])
        precipitations = hourly.get("precipitation", [])
        vents = hourly.get("wind_speed_10m", [])
        codes = hourly.get("weather_code", [])

        for i, timestamp in enumerate(temps):
            documents.append(
                {
                    "ville": ville,
                    "timestamp": timestamp,
                    "temperature_c": temperatures[i] if i < len(temperatures) else None,
                    "humidite_pct": humidites[i] if i < len(humidites) else None,
                    "precipitation_mm": precipitations[i] if i < len(precipitations) else None,
                    "vent_kmh": vents[i] if i < len(vents) else None,
                    "weather_code": codes[i] if i < len(codes) else None,
                }
            )
    return documents


def _deduplicate(documents: list[dict]) -> list[dict]:
    """Transformation 1 : supprime les doublons sur le couple (ville, timestamp)."""
    vus = set()
    uniques = []
    for doc in documents:
        cle = (doc["ville"], doc["timestamp"])
        if cle not in vus:
            vus.add(cle)
            uniques.append(doc)
    logger.info("Deduplication : %d -> %d documents", len(documents), len(uniques))
    return uniques


def _drop_missing(documents: list[dict]) -> list[dict]:
    """Transformation 2 : ecarte les lignes dont un champ obligatoire est manquant."""
    propres = [
        doc for doc in documents if all(doc.get(champ) is not None for champ in REQUIRED_FIELDS)
    ]
    logger.info("Gestion valeurs manquantes : %d -> %d documents", len(documents), len(propres))
    return propres


def _convert_types(documents: list[dict]) -> list[dict]:
    """Transformation 3 : force les types numeriques corrects."""
    for doc in documents:
        doc["temperature_c"] = float(doc["temperature_c"])
        doc["humidite_pct"] = float(doc["humidite_pct"])
        doc["precipitation_mm"] = float(doc.get("precipitation_mm") or 0.0)
        doc["vent_kmh"] = float(doc.get("vent_kmh") or 0.0)
        doc["weather_code"] = int(doc["weather_code"])
    return documents


def _normalize_dates(documents: list[dict]) -> list[dict]:
    """Transformation 4 : normalise le timestamp au format ISO 8601 complet."""
    for doc in documents:
        # Open-Meteo renvoie deja du ISO8601 sans secondes (ex: 2026-09-30T00:00)
        if len(doc["timestamp"]) == 16:
            doc["timestamp"] = doc["timestamp"] + ":00"
    return documents


def _add_condition_column(documents: list[dict]) -> list[dict]:
    """Transformation 5 : cree une nouvelle colonne 'condition' lisible a partir du code WMO."""
    for doc in documents:
        doc["condition"] = WEATHER_CODE_MAP.get(doc["weather_code"], "Condition inconnue")
        doc["id"] = f"{doc['ville']}_{doc['timestamp']}".replace(" ", "_").replace(":", "-")
    return documents


def transform(input_path: str, output_path: str) -> str:
    """Pipeline complet de transformation. Retourne le chemin du fichier transforme."""
    with open(input_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    documents = _flatten(raw)
    documents = _deduplicate(documents)
    documents = _drop_missing(documents)
    documents = _convert_types(documents)
    documents = _normalize_dates(documents)
    documents = _add_condition_column(documents)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(documents, f, ensure_ascii=False, indent=2)

    logger.info("Transformation terminee : %d documents ecrits dans %s", len(documents), output_path)
    return output_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    transform("/opt/airflow/data/raw_weather.json", "/opt/airflow/data/transformed_weather.json")
