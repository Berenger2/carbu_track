import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_name"),
    "user": os.getenv("DB_user"),
    "password": os.getenv("DB_password"),
    "host": os.getenv("DB_host"),
    "port": 5432,
}

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        raise RuntimeError(f"Erreur de connexion à la base de données : {e}")

# cursor
@contextmanager
def get_cursor():
    conn = get_db_connection()
    cursor = None
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        yield cursor
    finally:
        cursor.close()
        conn.close()