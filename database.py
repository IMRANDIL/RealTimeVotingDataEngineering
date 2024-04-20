import psycopg2
from psycopg2 import OperationalError

def connect_to_database():
    try:
        # Attempt to connect to the PostgreSQL database
        conn = psycopg2.connect(
            "host=localhost dbname=voting user=postgres password=postgres"
        )
        return conn
    except OperationalError as e:
        # If an operational error occurs, print an error message and return None
        print(f"OperationalError: {e}")
        return None

def create_tables(conn, cur):
    try:
        # Begin the transaction
        with conn:
            # Execute the CREATE TABLE statements
            cur.execute("""
                CREATE TABLE IF NOT EXISTS candidates (
                    candidate_id VARCHAR(255) PRIMARY KEY,
                    candidate_name VARCHAR(255),
                    party_affiliation VARCHAR(255),
                    biography TEXT,
                    campaign_platform TEXT,
                    photo_url TEXT
                )
            """)
            # Other CREATE TABLE statements...
    except psycopg2.DatabaseError as e:
        # Roll back the transaction if an exception occurs
        conn.rollback()
        print(f"Error occurred: {e}")
    else:
        # Commit the transaction if all statements execute successfully
        conn.commit()
        print("Tables created successfully!")
