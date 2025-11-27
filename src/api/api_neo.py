import os
import requests

API_KEY = os.getenv("API_KEY")
URL = f"https://api.nasa.gov/neo/rest/v1/feed?api_key={API_KEY}"

def fetch_neo_data():
    response = requests.get(URL, timeout=10)
    response.raise_for_status()
    data = response.json()
    formatted = []

    neo_data = data.get("near_earth_objects", {})
    for date, objects in neo_data.items():

        for obj in objects:
            obj_id = int(obj["id"])
            name = obj["name"]
            min_d = round(float(obj["estimated_diameter"]["meters"]["estimated_diameter_min"]), 2)
            max_d = round(float(obj["estimated_diameter"]["meters"]["estimated_diameter_max"]), 2)
            hazard = obj["is_potentially_hazardous_asteroid"]

            for approach in obj["close_approach_data"]:
                close_date = approach["close_approach_date"]
                miss_distance = round(float(approach["miss_distance"]["kilometers"]), 2)
                formatted.append((obj_id, name, min_d, max_d, hazard, close_date, miss_distance))

    return formatted
