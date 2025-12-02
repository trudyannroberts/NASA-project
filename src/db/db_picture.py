from psycopg2 import OperationalError, DatabaseError
from src.db.db import get_db


def create_space_picture_table():
    """
    Create the 'space_picture' table if it does not already exist.

    The table stores NASA's daily space picture along with metadata and a timestamp
    of when the record was last updated.

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
                CREATE TABLE IF NOT EXISTS space_picture (
                    date DATE PRIMARY KEY,
                    description TEXT,
                    copyright TEXT,
                    url TEXT
                )
            """)
            conn.commit()
        return conn, cur
    except (OperationalError, DatabaseError) as e:
        print(f"Database query error: {e}")

def insert_space_picture(date, description, copyright, url):
    try:
        with get_db() as (conn, cur):
            cur.execute("""
                INSERT INTO space_picture (date, description, copyright, url)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (date) DO NOTHING
                """, (date, description, copyright, url))

            conn.commit()
    except OperationalError as e:
        print(f"Database query error: {e}")

def fetch_picture():
    """Fetch all rows from space_picture."""
    try:
        with get_db() as (conn, cur):
            cur.execute("SELECT * FROM space_picture ORDER BY date DESC LIMIT 1")
            return cur.fetchall()
    except OperationalError as e:
        print(f"Database query error: {e}")
        return None

