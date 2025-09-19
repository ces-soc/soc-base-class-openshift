import os
from cessoc.postgresql import OpenshiftPostgresql
import psycopg2
import psycopg2.extras

class ItemNotFoundException(Exception):
    """Raised when the "Item" attribute is not present on a get response from DynamoDB"""
    pass

def connect_db(self):
        """Makes a DB connection"""  
        db_conn = OpenshiftPostgresql(
            database=os.environ["db_name"], 
            user=os.environ["db_username"],
            password=os.environ["db_password"],
            host=os.environ["host_name"],
            port=os.environ["port"]
            ).connection
        return db_conn


def create_table_if_not_exists():
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cessoc.timestamps (
            key TEXT PRIMARY KEY,
            lastRequest TEXT,
            newRequest TEXT,
            reportInfo JSONB,       
            streamPosition TEXT
        )
    """)

    conn.commit()
    conn.close()


def get(key: str) -> dict:
    """Gets a key. Raises ItemNotFoundException if the key does not exist."""

    create_table_if_not_exists()

    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM timestamps WHERE key = %s", (key,))
    result = cursor.fetchone()
    conn.close()

    if result:
        return result
    raise ItemNotFoundException("Could not find \"Item\" within PostgreSQL response")

def put(key: str, values: dict) -> None:
    """Inserts a key. Will overwrite everything at the key if the key exists."""

    create_table_if_not_exists()

    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM timestamps WHERE key = %s", (key,))
    result = cursor.fetchone()
    conn.close()

    if result:
        return result
    raise ItemNotFoundException("Item not found.")

def update(key: str, values: dict) -> None:
    """Updates individual columns at the key value. Will insert if the key does not exist."""

    create_table_if_not_exists()

    conn = connect_db()
    cursor = conn.cursor()

    set_clause = ', '.join([f"{k} = %s" for k in values])
    values_list = list(values.values()) + [key]
    cursor.execute(f"UPDATE timestamps SET {set_clause} WHERE key = %s", values_list)
    conn.commit()
    conn.close()
