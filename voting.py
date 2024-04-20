from database import connect_to_database




if __name__ == "__main__":
    conn = connect_to_database()
    
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
            print(candidates)
            
        except Exception as e:
            # Print a generic error message for any exceptions
            print(f"An error occurred: {e}")
            
        finally:
            conn.close()