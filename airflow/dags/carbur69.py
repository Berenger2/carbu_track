from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import os
from dotenv import load_dotenv
import logging

load_dotenv()

DB_HOST = os.getenv("DB_host")
DB_USER = os.getenv("DB_user")
DB_PASSWORD = os.getenv("DB_password")
DB_NAME = os.getenv("DB_name")

DEPARTEMENT = "69"

API_URL = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/exports/json"

# fetch données de l'API
def fetch_data_from_api(**kwargs):
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()
        
        # check la structure des données
        logging.info(f"Data structure from API: {data[:3]}")  # display 3 stations

        if not data:
            logging.warning("Aucune donnée récupérée de l'API.")
            return
        kwargs['ti'].xcom_push(key='raw_data', value=data)
        logging.info(f"Données récupérées avec succès: {len(data)} entrées.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur lors de la récupération des données de l'API : {e}")

# transform data
def transform_data(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='fetch_data')
    if not raw_data:
        logging.warning("Aucune donnée à transformer.")
        return
    
    # extract
    transformed_data = []
    
    for station in raw_data:
        station_data = {
            "id_station": station.get("id"),
            "adresse": station.get("adresse", "Inconnu"),
            "latitude": station.get("latitude"),
            "longitude": station.get("longitude"),
            "code_postal": station.get("cp"),
            "ville": station.get("ville"),
            "region": station.get("region"),
            "horaires_automate_24_24": station.get("horaires_automate_24_24"),
            "prix": [],
            "date_maj": []
        }

        # carburants disponibles
        carburants = [
            ("gazole", station.get("gazole_prix"), station.get("gazole_maj")),
            ("sp95", station.get("sp95_prix"), station.get("sp95_maj")),
            ("e85", station.get("e85_prix"), station.get("e85_maj")),
            ("gplc", station.get("gplc_prix"), station.get("gplc_maj")),
            ("e10", station.get("e10_prix"), station.get("e10_maj")),
            ("sp98", station.get("sp98_prix"), station.get("sp98_maj"))
        ]
        
        for carburant, prix, date_maj in carburants:
            if prix is not None:
                station_data["prix"].append({"type_carburant": carburant, "prix": prix, "date_maj": date_maj})
        
        # m'assure que caburant est disponibe avant d'ajouter la station
        if station_data["prix"]:  
            transformed_data.append(station_data)
    
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)
    logging.info(f"{len(transformed_data)} stations transformées avec succès.")

#  filtrer les station de la rhone 69
def filter_data(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    if not transformed_data:
        logging.warning("Aucune donnée à filtrer.")
        return
    filtered_data = [station for station in transformed_data if station.get('code_postal', '').startswith(DEPARTEMENT)]
    kwargs['ti'].xcom_push(key='filtered_data', value=filtered_data)
    logging.info(f"{len(filtered_data)} stations filtrées.")

# test 20 premières stations
def take_top_100(**kwargs):
    filtered_data = kwargs['ti'].xcom_pull(key='filtered_data', task_ids='filter_data')
    if not filtered_data:
        logging.warning("Aucune donnée à sélectionner.")
        return
    top_100_stations = filtered_data[:100]
    kwargs['ti'].xcom_push(key='top_100_data', value=top_100_stations)
    logging.info(f"{len(top_100_stations)} stations sélectionnées.")

# BDD
def check_and_create_table(**kwargs):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        logging.info("Connexion réussie à la base de données.")
        
        # check si la table existe, sinon create
        cursor.execute(""" 
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_name = 'prix_carburants_lyon'
            );
        """)
        exists = cursor.fetchone()[0]
        
        if not exists:
            logging.info("La table 'prix_carburants_lyon' n'existe pas, création de la table.")
            cursor.execute("""
                CREATE TABLE prix_carburants_lyon (
                    id_station BIGINT,
                    adresse VARCHAR(255),
                    ville VARCHAR(255),
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    type_carburant VARCHAR(50),
                    prix DOUBLE PRECISION,
                    date_maj TIMESTAMP,
                    horaires_automate_24_24 VARCHAR(50),
                    PRIMARY KEY (id_station, type_carburant, date_maj)  -- Définition de la clé primaire
                );
            """)
            logging.info("Table 'prix_carburants_lyon' créée avec succès.")
        
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Erreur lors de la connexion ou de la création de la table : {e}")

# persister 
def persist_data_to_db(**kwargs):
    top_100_data = kwargs['ti'].xcom_pull(key='top_100_data', task_ids='take_top_100')

    if not top_100_data:
        logging.warning("Aucune donnée à insérer dans la base de données.")
        return
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()
        logging.info("Connexion réussie à la base de données.")

        for station in top_100_data:
            for carburant in station["prix"]:
                try:
                    cursor.execute("""
                        INSERT INTO prix_carburants_lyon (id_station, adresse, ville, latitude, longitude, type_carburant, prix, date_maj, horaires_automate_24_24)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id_station, type_carburant, date_maj) DO NOTHING;  -- Utilisation de la clé unique sur (id_station, type_carburant, date_maj)
                    """, (
                        station['id_station'],
                        station.get('adresse', 'Inconnu'),
                        station['ville'],
                        station['latitude'],
                        station['longitude'],
                        carburant['type_carburant'],
                        carburant['prix'],
                        carburant['date_maj'],
                        station.get('horaires_automate_24_24', 'Non'),  
                    ))
                    logging.info(f"Station {station['id_station']} - Carburant {carburant['type_carburant']} inséré avec succès.")
                except Exception as e:
                    logging.error(f"Erreur lors de l'insertion de la station {station['id_station']} - Carburant {carburant['type_carburant']}: {e}")
        
        cursor.close()
        conn.close()
        logging.info("Insertion terminée.")
    except Exception as e:
        logging.error(f"Erreur lors de la connexion ou de l'insertion dans la base de données : {e}")

#  DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'suivi_prix_carburants_69',
    default_args=default_args,
    description='Pipeline ETL pour le suivi des prix des carburants à Lyon',
    schedule_interval='@daily', 
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    # 1 : fetch les données de l'API
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_api
    )

    # 2 : transform data
    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    # 3 : filtrer juste Lyon
    filter_data_task = PythonOperator(
        task_id='filter_data',
        python_callable=filter_data
    )

    # 4 : get les 100 first stations
    take_top_100_task = PythonOperator(
        task_id='take_top_100',
        python_callable=take_top_100
    )

    # 5 : bdd
    check_and_create_table_task = PythonOperator(
        task_id='check_and_create_table',
        python_callable=check_and_create_table
    )

    # 6 : persister
    persist_data = PythonOperator(
        task_id='persist_data',
        python_callable=persist_data_to_db
    )

    # mes tâches
    fetch_data >> transform_data_task >> filter_data_task >> take_top_100_task >> check_and_create_table_task >> persist_data
