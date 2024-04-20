from database import connect_to_database




if __name__ == "__main__":
    conn = connect_to_database()
    
    # select row_to_json(col)
    # from (select * from candidates) as col; _________This would also work
    
    if conn:
        try:
            cursor = conn.cursor()
            
            candidates_query = cursor.execute("""
                                              
                                             SELECT json_build_object(
           'candidate_id', candidate_id,
           'candidate_name', candidate_name,
           'party_affiliation', party_affiliation,
           'biography', biography,
           'campaign_platform', campaign_platform,
           'photo_url', photo_url
       ) AS candidate_json
FROM candidates;
""")
            candidates = [candidate[0] for candidate in cursor.fetchall()]
            
            if len(candidates) == 0:
                raise Exception("No candidates found in the database.")
            
            
        except Exception as e:
            # Print a generic error message for any exceptions
            print(f"An error occurred: {e}")
            
        finally:
            conn.close()