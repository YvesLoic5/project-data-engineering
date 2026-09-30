"""
Module de chargement : cree (si besoin) l'index Elasticsearch 'meteo_madagascar'
avec son mapping, puis charge les documents valides via l'API _bulk.
"""

import json
import logging
import os
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

ES_HOST = os.environ.get("ES_HOST", "http://elasticsearch:9200")
ES_USER = os.environ.get("ES_USER", "elastic")
ES_PASSWORD = os.environ.get("ES_PASSWORD", "changeme123")
INDEX_NAME = "meteo_madagascar"

MAPPING_PATH = Path(__file__).resolve().parent.parent / "elasticsearch" / "mapping.json"


def _auth():
    return (ES_USER, ES_PASSWORD)


def ensure_index() -> None:
    """Cree l'index avec son mapping s'il n'existe pas deja."""
    resp = requests.head(f"{ES_HOST}/{INDEX_NAME}", auth=_auth(), timeout=30)
    if resp.status_code == 200:
        logger.info("Index '%s' deja existant, mapping non recree.", INDEX_NAME)
        return

    with open(MAPPING_PATH, "r", encoding="utf-8") as f:
        mapping = json.load(f)

    resp = requests.put(
        f"{ES_HOST}/{INDEX_NAME}",
        auth=_auth(),
        json=mapping,
        timeout=30,
    )
    resp.raise_for_status()
    logger.info("Index '%s' cree avec succes.", INDEX_NAME)


def load(input_path: str) -> dict:
    """Charge les documents transformes et valides dans Elasticsearch via _bulk.

    Retourne un resume (nombre de documents indexes / erreurs).
    """
    ensure_index()

    with open(input_path, "r", encoding="utf-8") as f:
        documents = json.load(f)

    lignes_bulk = []
    for doc in documents:
        lignes_bulk.append(json.dumps({"index": {"_index": INDEX_NAME, "_id": doc["id"]}}))
        lignes_bulk.append(json.dumps(doc, ensure_ascii=False))

    payload = "\n".join(lignes_bulk) + "\n"

    resp = requests.post(
        f"{ES_HOST}/_bulk",
        auth=_auth(),
        data=payload.encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        timeout=60,
    )
    resp.raise_for_status()
    result = resp.json()

    erreurs = [item for item in result.get("items", []) if item["index"].get("status", 200) >= 300]
    resume = {
        "documents_envoyes": len(documents),
        "documents_en_erreur": len(erreurs),
        "took_ms": result.get("took"),
    }
    logger.info("Chargement Elasticsearch termine : %s", resume)

    if erreurs:
        raise RuntimeError(f"{len(erreurs)} documents ont echoue lors du chargement bulk : {erreurs[:3]}")

    return resume


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load("/opt/airflow/data/transformed_weather.json")
