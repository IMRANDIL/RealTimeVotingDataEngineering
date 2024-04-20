from database import connect_to_database, create_tables, fetch_All_Candidates
from utils_db import generate_candidate, generate_voter_data, insert_voters

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
                    print('candidate_______________', candidate)
                    
                    # Execute the INSERT INTO statement with proper formatting
                    curr.execute("""
                                 INSERT INTO candidates(
                                     candidate_id,
                                     candidate_name,
                                     party_affiliation,
                                     biography,
                                     campaign_platform,
                                     photo_url
                                 )
                                 VALUES(
                                     %(candidate_id)s,
                                     %(candidate_name)s,
                                     %(party_affiliation)s,
                                     %(biography)s,
                                     %(campaign_platform)s,
                                     %(photo_url)s
                                 )
                                 """, candidate)  # Pass the candidate dictionary as parameters
            
            # generate voters data now
            voters_data = []
            for i in range(1000):
                voter_data = generate_voter_data()
                if voter_data:
                    voters_data.append(voter_data)
                    if len(voters_data) == 60:
                        insert_voters(conn, curr, voters_data)
                        voters_data = []
            
            # Insert remaining voters if any
            if voters_data:
                insert_voters(conn, curr, voters_data)
            # Commit the transaction
            conn.commit()
            
        except Exception as e:
            # Print a generic error message for any exceptions
            print(f"An error occurred: {e}")
        finally:
            # Close the database connection
            conn.close()
