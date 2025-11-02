import os
import requests

API_KEY = os.getenv("API_KEY")
URL = f"https://api.nasa.gov/insight_weather/?api_key={API_KEY}&feedtype=json&ver=1.0"

def fetch_mars_data():
    response = requests.get(URL, timeout=10)
    response.raise_for_status()
    data = response.json()
    sols = data.get("sol_keys", [])
    formatted = []

    for sol in sols:
        sol_data = data[sol]["AT"]
        avg_temp = round(sol_data["av"], 1)
        max_temp = round(sol_data["mx"], 1)
        min_temp = round(sol_data["mn"], 1)
        date = data[sol]["First_UTC"].split("T")[0]
        formatted.append((sol, date, max_temp, min_temp, avg_temp))

    return formatted