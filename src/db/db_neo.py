from psycopg2 import OperationalError, DatabaseError
from src.db.db import get_db


def create_near_earth_objects_table():
    """
    Connect to the database and create the `near_earth_object` table if it doesn't exist.

    The table stores data on Near-Earth Objects (NEOs) and logs each action:
        - updated: timestamp of insertion (DEFAULT NOW())

    Returns
    -------
    conn : psycopg2.connection
        Active database connection.
    cur : psycopg2.cursor
        Database cursor for executing queries.
    """
    try:
        with get_db() as (conn, cur):
            cur.execute("""
                CREATE TABLE IF NOT EXISTS near_earth_object (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    min_diameter_meters REAL,
                    max_diameter_meters REAL,
                    is_potential_hazard BOOLEAN,
                    close_approach_date DATE,
                    miss_distance_km REAL,
                    updated TIMESTAMP DEFAULT NOW()
                )
            """)
            conn.commit()
            return conn, cur
    except OperationalError as e:
        print(f"Database connection error: {e}")
    except DatabaseError as e:
        print(f"Database query error: {e}")

def insert_near_earth_objects(rows):
    """
    Insert multiple Near-Earth Object (NEO) records into the database.

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
        with get_db() as (conn, cur):
            cur.executemany("""
                INSERT INTO near_earth_object (
                    id, name, min_diameter_meters, max_diameter_meters, is_potential_hazard, close_approach_date, miss_distance_km
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, rows)

            conn.commit()
    except OperationalError as e:
        print(f"Database query error: {e}")

def fetch_neo():
    """Fetch all rows near_earth_objects"""
    try:
        with get_db() as (conn, cur):
            cur.execute("SELECT * FROM near_earth_object ORDER BY id DESC")
            return cur.fetchall()
    except OperationalError as e:
        print(f"Database query error: {e}")
        return None
