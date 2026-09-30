"""
DAG Airflow : pipeline ETL Meteo Madagascar

Chaine complete : Extraction (API Open-Meteo) -> Transformation ->
Validation qualite -> Chargement Elasticsearch.

Fonctionnalites Airflow demontrees :
- planification quotidienne (schedule) ;
- plusieurs taches avec dependances explicites ;
- PythonOperator pour chaque etape ;
- retries automatiques en cas d'echec (ex : API temporairement indisponible) ;
- logs detailles a chaque etape ;
- XCom pour transmettre les chemins de fichiers et le rapport qualite entre taches ;
- tache dediee au controle de la qualite des donnees.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

import sys

sys.path.append("/opt/airflow/scripts")

DATA_DIR = "/opt/airflow/data"

default_args = {
    "owner": "data_engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="etl_pipeline_meteo",
    description="Pipeline ETL Meteo Madagascar : Open-Meteo -> Elasticsearch -> Kibana",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["final_project", "meteo", "etl"],
)
def etl_pipeline_meteo():
    @task(retries=3, retry_delay=timedelta(minutes=1))
    def extraction(**context):
        """Recupere les previsions meteo horaires pour 6 villes de Madagascar."""
        from extract import extract_all

        run_id = context["run_id"].replace(":", "-").replace("+", "-")
        chemin_sortie = f"{DATA_DIR}/raw_weather_{run_id}.json"
        chemin = extract_all(chemin_sortie)
        print(f"[extraction] Fichier brut ecrit : {chemin}")
        return chemin

    @task
    def transformation(chemin_brut: str, **context):
        """Nettoie et enrichit les donnees (dedoublonnage, types, dates, condition)."""
        from transform import transform

        run_id = context["run_id"].replace(":", "-").replace("+", "-")
        chemin_sortie = f"{DATA_DIR}/transformed_weather_{run_id}.json"
        chemin = transform(chemin_brut, chemin_sortie)
        print(f"[transformation] Fichier transforme ecrit : {chemin}")
        return chemin

    @task
    def controle_qualite(chemin_transforme: str):
        """Verifie la qualite des donnees avant chargement. Echoue le DAG si non conforme."""
        from validate import validate

        rapport = validate(chemin_transforme)
        print(f"[controle_qualite] Rapport : {rapport}")
        return rapport

    @task
    def chargement_elasticsearch(chemin_transforme: str, rapport_qualite: dict):
        """Charge les documents valides dans Elasticsearch (index meteo_madagascar)."""
        from load import load

        print(f"[chargement] Qualite validee : {rapport_qualite['statut']}")
        resume = load(chemin_transforme)
        print(f"[chargement] Resume : {resume}")
        return resume

    # Ordonnancement du graphe avec passage de donnees via XCom (retours de taches)
    chemin_brut = extraction()
    chemin_transforme = transformation(chemin_brut)
    rapport_qualite = controle_qualite(chemin_transforme)
    chargement_elasticsearch(chemin_transforme, rapport_qualite)


etl_pipeline_meteo()
