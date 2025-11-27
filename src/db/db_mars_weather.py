import os
from dotenv import load_dotenv
from psycopg2 import connect, OperationalError, DatabaseError

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

conn = connect(DATABASE_URL)
cur = conn.cursor()

def create_mars_weather_table():
    """Connect to DB and create Mars weather table if it doesn't exist.
    The table stores data on temperatures on Mars and logs updates

    Returns
    -------
    conn : psycopg2.connection
        Database connection object.
    cur : psycopg2.cursor
        Database cursor object.
    """
    try:
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

def insert_mars_weather(rows):
    """
    Insert multiple Mars weather records into the database.

    Automatically logs the action:
        - updated: current timestamp (DEFAULT NOW())

    Parameters
    ----------
    cur : psycopg2.cursor
        Database cursor.
    rows : list of tuples
        Each tuple: (id, name, min_diameter_meters, max_diameter_meters,
                     is_potential_hazard, close_approach_date, miss_distance_km)
    """
    try:
        cur.executemany("""
        INSERT INTO mars_weather (sol, date, max_temp, min_temp, avg_temp)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (sol) DO NOTHING
    """, rows)

        conn.commit()
        cur.close()
        conn.close()
    except (OperationalError) as e:
        print(f"Database query error:  {e}")