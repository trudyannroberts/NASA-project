import os
from dotenv import load_dotenv
from psycopg2 import connect, OperationalError, DatabaseError

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def create_mars_weather_table():
    """ Connect to the database that contains data on Mars weather. Return a database connection."""
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
                updated TIMESTAMP
            )
        """)
        conn.commit()
        return conn, cur
    except OperationalError as e:
        print(f"Database connection error: {e}")
    except DatabaseError as e:
        print(f"Database query error: {e}")

def create_near_earth_objects_table():
    """ Connect to the database that contains data on objects near Earth.Return a database connection."""
    try:
        conn = connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS near_earth_object (
                id INTEGER PRIMARY KEY,
                name TEXT,
                min_diameter REAL,
                max_diameter REAL,
                is_potential_hazard BOOLEAN,
                close_approach_date DATE,
                miss_distance REAL
            )
        """)
        conn.commit()
        return conn, cur
    except OperationalError as e:
        print(f"Database connection error: {e}")
    except DatabaseError as e:
        print(f"Database query error: {e}")

