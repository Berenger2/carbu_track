from fastapi import APIRouter, HTTPException, Query
from src.database import get_db_connection

router = APIRouter()

# all stations
@router.get("/stations", tags=["Stations"])
def get_all_stations()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = """
        SELECT id_station, adresse, ville, latitude, longitude, horaires_automate_24_24, 
               type_carburant, prix, date_maj
        FROM prix_carburants_lyon
        ORDER BY id_station, type_carburant
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        # transform
        stations = {}
        for row in rows:
            id_station = row['id_station']
            if id_station not in stations:
                stations[id_station] = {
                    "id_station": id_station,
                    "adresse": row['adresse'],
                    "ville": row['ville'],
                    "latitude": row['latitude'],
                    "longitude": row['longitude'],
                    "horaires_automate_24_24": row['horaires_automate_24_24'],
                    "carburants": []
                }
            stations[id_station]["carburants"].append({
                "type_carburant": row['type_carburant'],
                "prix": row['prix'],
                "date_maj": row['date_maj'],
            })

        return {"stations": list(stations.values())}
    finally:
        cursor.close()
        conn.close()



#  10 station moins chères du 69
@router.get("/stations/top10", tags=["Stations"])
def get_top10_stations(type_carburant: str = Query(..., description="Type de carburant (gazole, SP95, E10, etc.)")):

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = f"""
        SELECT id_station, adresse, ville, latitude, longitude, type_carburant, prix, date_maj,horaires_automate_24_24
        FROM prix_carburants_lyon
        WHERE type_carburant = %s
        ORDER BY prix ASC
        LIMIT 10
        """
        cursor.execute(query, (type_carburant,))
        stations = cursor.fetchall()
        if not stations:
            raise HTTPException(status_code=404, detail="Aucune station trouvée pour ce type de carburant.")
        return {"top_10_stations": stations}
    finally:
        cursor.close()
        conn.close()


# recherche par ville
@router.get("/stations/ville/{ville}", tags=["Stations"])
def get_stations_by_ville(ville: str):

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query_stations = """
        SELECT id_station, adresse, ville, latitude, longitude, horaires_automate_24_24, 
               type_carburant, prix, date_maj
        FROM prix_carburants_lyon 
        WHERE LOWER(ville) = LOWER(%s)
        ORDER BY id_station, type_carburant
        """
        cursor.execute(query_stations, (ville,))
        rows = cursor.fetchall()

        stations = {}
        for row in rows:
            id_station = row['id_station']
            if id_station not in stations:
                stations[id_station] = {
                    "id_station": id_station,
                    "adresse": row['adresse'],
                    "ville": row['ville'],
                    "latitude": row['latitude'],
                    "longitude": row['longitude'],
                    "horaires_automate_24_24": row['horaires_automate_24_24'],
                    "carburants": []
                }
            stations[id_station]["carburants"].append({
               "type_carburant": row['type_carburant'],
                "prix": row['prix'],
                "date_maj": row['date_maj'],
            })

        stations_list = list(stations.values())

        return {"stations": stations_list}
    finally:
        cursor.close()
        conn.close()


# 10 station moins chères de la ville
@router.get("/stations/ville/{ville}/top10", tags=["Stations"])
def get_top10_stations_by_ville(ville: str, type_carburant: str = Query(..., description="Type de carburant pour le Top 10")):

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query_top10 = """
        SELECT id_station, adresse, ville, type_carburant, prix, date_maj
        FROM prix_carburants_lyon
        WHERE LOWER(ville) = LOWER(%s) AND type_carburant = %s
        ORDER BY prix ASC
        LIMIT 10
        """
        cursor.execute(query_top10, (ville, type_carburant))
        rows = cursor.fetchall()

        top_10_stations = [
            {
                "id_station": row['id_station'],
                "adresse": row['adresse'],
                "ville": row['ville'],
                "type_carburant": row['type_carburant'],
                "prix": row['prix'],
                "date_maj": row['date_maj']
            }
            for row in rows
        ]

        return {"top_10_stations": top_10_stations}
    finally:
        cursor.close()
        conn.close()