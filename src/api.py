# main.py
import requests
import os
from dotenv import load_dotenv
from db import get_connection

load_dotenv()
API_KEY = os.getenv("API_KEY")
url = f"https://api.nasa.gov/insight_weather/?api_key={API_KEY}&feedtype=json&ver=1.0"

response = requests.get(url)
data = response.json()
sols = data['sol_keys']

conn, cur = get_connection()

for sol in sols:
    sol_data = data[sol]['AT']
    avg_temp = round(sol_data['av'], 1)
    max_temp = round(sol_data['mx'], 1)
    min_temp = round(sol_data['mn'], 1)
    date = data[sol]['First_UTC'].split('T')[0]

    cur.execute("""
        INSERT INTO mars_weather (sol, date, max_temp, min_temp, avg_temp)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (sol) DO NOTHING
    """, (sol, date, max_temp, min_temp, avg_temp))

conn.commit()
cur.close()
conn.close()
