from db import get_db
from psycopg2 import OperationalError, DatabaseError


def create_mars_weather_table():
    """Create Mars weather table if it doesn't exist."""
    try:
        with get_db() as (conn, cur):
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
    except (OperationalError, DatabaseError) as e:
        print(f"DB error: {e}")



def insert_mars_weather(rows):
    """Insert multiple Mars weather records."""
    try:
        with get_db() as (conn, cur):
            cur.executemany("""
                INSERT INTO mars_weather (sol, date, max_temp, min_temp, avg_temp)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (sol) DO NOTHING
            """, rows)
            conn.commit()
    except OperationalError as e:
        print(f"Database query error: {e}")



def fetch_mars_weather():
    """Fetch all rows from mars_weather."""
    try:
        with get_db() as (conn, cur):
            cur.execute("SELECT * FROM mars_weather ORDER BY sol DESC")
            return cur.fetchall()
    except OperationalError as e:
        print(f"Database query error: {e}")
        return None
