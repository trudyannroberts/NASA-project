import os
import requests
from db import create_near_earth_objects_table

API_KEY = os.getenv("API_KEY")
url = f"https://api.nasa.gov/neo/rest/v1/feed?api_key={API_KEY}"

try:
    response = requests.get(url)
    data = response.json()
    conn, cur = create_near_earth_objects_table()

    neo = data["near_earth_objects"]

    for date, objects in neo.items():
        for obj in objects:

            id = obj["id"]
            name = obj["name"]
            min_diameter = round(float(obj["estimated_diameter"]['meters']['estimated_diameter_min']), 2)
            max_diameter = round(float(obj["estimated_diameter"]['meters']['estimated_diameter_max']), 2)
            is_potential_hazard = "Yes" if obj["is_potentially_hazardous_asteroid"] else "No"

            for data in obj["close_approach_data"]:
                close_date = data['close_approach_date']
                miss_distance = round(float(data["miss_distance"]['kilometers']), 2)

                cur.execute("""
                    INSERT INTO near_earth_object (id, name, min_diameter, max_diameter, is_potential_hazard, close_approach_date, miss_distance)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (id, name, min_diameter, max_diameter, is_potential_hazard, close_date, miss_distance))

        conn.commit()
        cur.close()
        conn.close()
except requests.exceptions.Timeout:
    print("Request timed out")
except requests.exceptions.HTTPError as err:
    print(f"HTTP error: {err}")
except requests.exceptions.RequestException as err:
    print(f"Other request error: {err}")
except Exception as e:
    print(f"Unexpected error: {e}")