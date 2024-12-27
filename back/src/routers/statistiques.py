from fastapi import APIRouter, HTTPException, Query
from src.database import get_db_connection

router = APIRouter()

# prix moyen par type de carburant
@router.get("/stats/moyennes", tags=["Statistiques"])
def get_avg_prices():

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = """
        SELECT type_carburant, AVG(prix) as prix_moyen
        FROM prix_carburants_lyon
        GROUP BY type_carburant
        """
        cursor.execute(query)
        averages = cursor.fetchall()
        return {"prix_moyens": averages}
    finally:
        cursor.close()
        conn.close()