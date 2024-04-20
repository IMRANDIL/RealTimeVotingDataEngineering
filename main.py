import json
from confluent_kafka import SerializingProducer
from database import connect_to_database, create_tables, fetch_All_Candidates
from utils_db import generate_candidate, generate_voter_data, insert_voters






def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")







if __name__ == "__main__":
    # Connect to the database
    conn = connect_to_database()
    
    #add kafka into picture now
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092'
    })

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
            
           # Generate voters data now
            voters_data = []
            candidate_ids_set = set()  # Set to keep track of unique candidate IDs in the batch
            for i in range(1000):
                voter_data = generate_voter_data()
                if voter_data:
                    candidate_id = voter_data['voter_id']
                    # Check if the candidate ID is not in the set already
                    if candidate_id not in candidate_ids_set:
                        # Add the candidate ID to the set
                        candidate_ids_set.add(candidate_id)
                        # Produce to voters_topic
                        producer.produce(
                            "voters_topic",
                            key=voter_data['voter_id'],
                            value=json.dumps(voter_data),
                            on_delivery=delivery_report
                        )
                        voters_data.append(voter_data)
                        # If the batch size reaches 60 or the last iteration, insert the voters data
                        if len(voters_data) == 60 or i == 999:
                            insert_voters(conn, curr, voters_data)
                            print('length of voters_data', len(voters_data))
                            voters_data = []  # Reset the voters data list
            # Commit the transaction
            conn.commit()
            
        except Exception as e:
            # Print a generic error message for any exceptions
            print(f"An error occurred: {e}")
        finally:
            # Close the database connection
            conn.close()
            
            # Flush any remaining buffered messages to Kafka
            producer.flush()
