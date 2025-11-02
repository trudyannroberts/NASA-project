import os
from dotenv import load_dotenv
from psycopg2 import connect, OperationalError, DatabaseError

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def create_near_earth_objects_table():
    """ Connect to the database that contains data on objects near Earth.Return a database connection."""
    try:
        conn = connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS near_earth_object (
                id INTEGER PRIMARY KEY,
                name TEXT,
                min_diameter_meters REAL,
                max_diameter_meters REAL,
                is_potential_hazard BOOLEAN,
                close_approach_date DATE,
                miss_distance_km REAL
            )
        """)
        conn.commit()
        return conn, cur
    except OperationalError as e:
        print(f"Database connection error: {e}")
    except DatabaseError as e:
        print(f"Database query error: {e}")

def insert_near_earth_objects(cur, rows):
    """Batch-insert multiple NEO rows."""
    cur.executemany("""
        INSERT INTO near_earth_object (
            id, name, min_diameter_meters, max_diameter_meters, is_potential_hazard, close_approach_date, miss_distance_km
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """, rows)

