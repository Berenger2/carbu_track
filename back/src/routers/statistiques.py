from fastapi import APIRouter, HTTPException
from src.database import get_cursor

router = APIRouter()

# prix moyen par type de carburant
@router.get("/stats/moyennes", tags=["Statistiques"], responses={
    200: {"description": "Prix moyen par type de carburant"},
    500: {"description": "Erreur serveur"},
})
def get_avg_prices():
      with get_cursor() as cursor:
        query = """
        SELECT type_carburant, AVG(prix) AS prix_moyen
        FROM prix_carburants_lyon
        GROUP BY type_carburant
        """
        try:
            cursor.execute(query)
            rows = cursor.fetchall()

            if not rows:
                raise HTTPException(status_code=404, detail="Aucune donnée disponible pour calculer les prix moyens.")

            averages = [
                {"type_carburant": row["type_carburant"], "prix_moyen": round(row["prix_moyen"], 2)}
                for row in rows
            ]
            return {"prix_moyens": averages}

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erreur serveur : {str(e)}")