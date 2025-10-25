import os
import requests
from db import create_mars_weather_table

API_KEY = os.getenv("API_KEY")
url = f"https://api.nasa.gov/insight_weather/?api_key={API_KEY}&feedtype=json&ver=1.0"

def fetch_and_store_mars_data():
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        sols = data.get('sol_keys', [])

        conn, cur = create_mars_weather_table()

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

            print(f'Sol {sol} ({date}): avg {avg_temp}°C, max {max_temp}°C, min {min_temp}°C')

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

if __name__ == "__main__":
    fetch_and_store_mars_data()
