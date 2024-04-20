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
            cur.execute("""
                CREATE TABLE IF NOT EXISTS voters (
                    voter_id VARCHAR(255) PRIMARY KEY,
                    voter_name VARCHAR(255),
                    date_of_birth VARCHAR(255),
                    gender VARCHAR(255),
                    nationality VARCHAR(255),
                    registration_number VARCHAR(255),
                    address_street VARCHAR(255),
                    address_city VARCHAR(255),
                    address_state VARCHAR(255),
                    address_country VARCHAR(255),
                    address_postcode VARCHAR(255),
                    email VARCHAR(255),
                    phone_number VARCHAR(255),
                    cell_number VARCHAR(255),
                    picture TEXT,
                    registered_age INTEGER
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS votes (
                    voter_id VARCHAR(255) UNIQUE,
                    candidate_id VARCHAR(255),
                    voting_time TIMESTAMP,
                    vote int DEFAULT 1,
                    PRIMARY KEY (voter_id, candidate_id)
                )
            """)
    except psycopg2.DatabaseError as e:
        # Roll back the transaction if an exception occurs
        conn.rollback()
        print(f"Error occurred: {e}")
    else:
        # Commit the transaction if all statements execute successfully
        conn.commit()
        print("Tables created successfully!")
