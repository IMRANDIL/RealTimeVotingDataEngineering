from database import connect_to_database, create_tables, fetch_All_Candidates
from utils_db import generate_candidate
if __name__ == "__main__":
    # Connect to the database
    conn = connect_to_database()

    if conn:
        try:
            # Print a success message if connection is successful
            print("Successfully connected to the PostgreSQL database!")
            
            # Create a cursor object
            curr = conn.cursor()
            
            # Call the function to create tables
            create_tables(conn, curr)
            
            # Fetch all candidates
            candidates = fetch_All_Candidates(conn,curr)
            
            if len(candidates) == 0:
                for i in range(3):
                    candidate = generate_candidate(i,3)
            
            # Commit the transaction
            conn.commit()
            
        except Exception as e:
            # Print a generic error message for any exceptions
            print(f"An error occurred: {e}")
        finally:
            # Close the database connection
            conn.close()
