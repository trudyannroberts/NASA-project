from contextlib import contextmanager
from psycopg2 import connect
import os
from dotenv import load_dotenv


load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

@contextmanager
def get_db():
    conn = connect(DATABASE_URL)
    cur = conn.cursor()
    try:
        yield conn, cur
    finally:
        cur.close()
        conn.close()