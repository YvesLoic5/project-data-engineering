"""
Module d'extraction : recupere les previsions meteo horaires
pour plusieurs villes de Madagascar via l'API publique Open-Meteo
(https://open-meteo.com/), qui ne necessite aucune cle d'API.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

API_URL = "https://api.open-meteo.com/v1/forecast"

# Villes suivies : nom, latitude, longitude
VILLES = [
    {"ville": "Antananarivo", "latitude": -18.8792, "longitude": 47.5079},
    {"ville": "Toamasina", "latitude": -18.1492, "longitude": 49.4023},
    {"ville": "Antsirabe", "latitude": -19.8667, "longitude": 47.0333},
    {"ville": "Mahajanga", "latitude": -15.7167, "longitude": 46.3167},
    {"ville": "Fianarantsoa", "latitude": -21.4536, "longitude": 47.0854},
    {"ville": "Toliara", "latitude": -23.35, "longitude": 43.6667},
]

HOURLY_VARS = "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,weather_code"


def extract_ville(ville: dict, forecast_days: int = 2) -> dict:
    """Interroge l'API Open-Meteo pour une ville donnee et renvoie le JSON brut."""
    params = {
        "latitude": ville["latitude"],
        "longitude": ville["longitude"],
        "hourly": HOURLY_VARS,
        "timezone": "auto",
        "forecast_days": forecast_days,
    }
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    payload["ville"] = ville["ville"]
    return payload


def extract_all(output_path: str) -> str:
    """Extrait les donnees de toutes les villes et les sauvegarde en JSON brut.

    Retourne le chemin du fichier ecrit (utile pour le passage via XCom).
    """
    resultats = []
    for ville in VILLES:
        try:
            data = extract_ville(ville)
            resultats.append(data)
            logger.info("Extraction reussie pour %s", ville["ville"])
        except requests.RequestException as exc:
            logger.error("Echec extraction pour %s : %s", ville["ville"], exc)
            raise

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            {"extracted_at": datetime.utcnow().isoformat(), "villes": resultats},
            f,
            ensure_ascii=False,
            indent=2,
        )
    logger.info("Donnees brutes ecrites dans %s", output_path)
    return output_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_all("/opt/airflow/data/raw_weather.json")
