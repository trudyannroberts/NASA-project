import os
from dotenv import load_dotenv
from psycopg2 import connect, OperationalError, DatabaseError

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def create_mars_weather_table():
    """Connect to DB and create Mars weather table if it doesn't exist."""
    try:
        conn = connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mars_weather (
                sol INTEGER PRIMARY KEY,
                date DATE,
                max_temp REAL,
                min_temp REAL,
                avg_temp REAL,
                updated TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
        return conn, cur
    except (OperationalError, DatabaseError) as e:
        print(f"DB error: {e}")

def insert_mars_weather(cur, rows):
    """Batch-insert multiple Mars weather rows."""
    cur.executemany("""
        INSERT INTO mars_weather (sol, date, max_temp, min_temp, avg_temp)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (sol) DO NOTHING
    """, rows)
