import os
from dotenv import load_dotenv
from psycopg2 import connect, OperationalError, DatabaseError

load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

conn = connect(DATABASE_URL)
cur = conn.cursor()

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
    except OperationalError as e:
        print(f"Database connection error: {e}")
    except DatabaseError as e:
        print(f"Database query error: {e}")

def insert_space_picture(date, description, copyright, url):
    try:
        cur.execute("""
            INSERT INTO space_picture (date, description, copyright, url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING
            """, (date, description, copyright, url))

        conn.commit()
        cur.close()
        conn.close()
    except OperationalError as e:
        print(f"Database query error: {e}")


