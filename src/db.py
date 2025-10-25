import os
from dotenv import load_dotenv
from psycopg2 import connect

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    conn = connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mars_weather (
            sol INTEGER PRIMARY KEY,
            date DATE,
            max_temp REAL,
            min_temp REAL,
            avg_temp REAL
        )
    """)
    conn.commit()
    return conn, cur