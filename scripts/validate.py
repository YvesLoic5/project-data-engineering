"""
Module de validation / controle qualite des donnees transformees.

Verifie :
- le nombre minimal de lignes attendu ;
- la presence des champs obligatoires ;
- les types des champs numeriques ;
- l'unicite de l'identifiant 'id' ;
- des plages de valeurs plausibles (temperature, humidite).

Leve une exception ValueError si un controle critique echoue, ce qui fait
echouer la tache Airflow correspondante (et declenche les retries).
"""

import json
import logging

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = [
    "id",
    "ville",
    "timestamp",
    "temperature_c",
    "humidite_pct",
    "precipitation_mm",
    "vent_kmh",
    "weather_code",
    "condition",
]

MIN_ROWS = 50
TEMP_RANGE = (-10.0, 55.0)
HUMIDITE_RANGE = (0.0, 100.0)


def validate(input_path: str) -> dict:
    """Execute tous les controles qualite. Retourne un rapport dict et leve
    ValueError en cas d'echec bloquant.
    """
    with open(input_path, "r", encoding="utf-8") as f:
        documents = json.load(f)

    rapport = {
        "nombre_lignes": len(documents),
        "erreurs": [],
    }

    # 1. Nombre de lignes minimal
    if len(documents) < MIN_ROWS:
        rapport["erreurs"].append(
            f"Nombre de lignes insuffisant : {len(documents)} < {MIN_ROWS}"
        )

    # 2. Champs obligatoires presents
    for i, doc in enumerate(documents):
        champs_manquants = [c for c in REQUIRED_FIELDS if c not in doc]
        if champs_manquants:
            rapport["erreurs"].append(
                f"Ligne {i} ({doc.get('id')}) : champs manquants {champs_manquants}"
            )

    # 3. Types numeriques corrects
    for doc in documents:
        if not isinstance(doc.get("temperature_c"), (int, float)):
            rapport["erreurs"].append(f"{doc.get('id')} : temperature_c n'est pas numerique")
        if not isinstance(doc.get("humidite_pct"), (int, float)):
            rapport["erreurs"].append(f"{doc.get('id')} : humidite_pct n'est pas numerique")

    # 4. Unicite de l'identifiant
    ids = [doc["id"] for doc in documents if "id" in doc]
    doublons = {i for i in ids if ids.count(i) > 1}
    if doublons:
        rapport["erreurs"].append(f"Identifiants dupliques detectes : {doublons}")

    # 5. Plages de valeurs plausibles
    for doc in documents:
        temp = doc.get("temperature_c")
        if temp is not None and not (TEMP_RANGE[0] <= temp <= TEMP_RANGE[1]):
            rapport["erreurs"].append(f"{doc.get('id')} : temperature hors plage ({temp})")
        humidite = doc.get("humidite_pct")
        if humidite is not None and not (HUMIDITE_RANGE[0] <= humidite <= HUMIDITE_RANGE[1]):
            rapport["erreurs"].append(f"{doc.get('id')} : humidite hors plage ({humidite})")

    rapport["nombre_erreurs"] = len(rapport["erreurs"])
    rapport["statut"] = "OK" if not rapport["erreurs"] else "ECHEC"

    logger.info("Rapport de validation : %s", rapport)

    if rapport["erreurs"]:
        raise ValueError(
            f"Controle qualite echoue ({rapport['nombre_erreurs']} erreurs) : "
            f"{rapport['erreurs'][:5]}"
        )

    return rapport


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    validate("/opt/airflow/data/transformed_weather.json")
