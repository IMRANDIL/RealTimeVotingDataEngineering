import datetime
import random
import json
from database import connect_to_database
from confluent_kafka import Consumer, KafkaError, SerializingProducer

conf = {
    "bootstrap.servers": "localhost:9092"
}

consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf)



def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")







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
            
            if len(candidates) == 0:
                raise Exception("No candidates found in the database.")
            
            consumer.subscribe(['voters_topic'])
            
            processed_messages = 0
            commit_interval = 10  # Commit every 10 messages
            
            try: 
                while True:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    elif msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition, ignore
                            continue
                        else:
                            print(msg.error())
                            break
                    else:
                        # Process the message
                        voter = json.loads(msg.value().decode('utf-8'))
                        chosen_candidate = random.choice(candidates)
                        
                        # Merge voter data, candidate data, and additional vote information
                        vote = {
                            **voter,
                            **chosen_candidate,
                            'voting_time': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                            'vote': 1   
                        }
                        
                        try:
                            print(f"User {vote['voter_id']} is voting for candidate: {vote['candidate_id']}")
                            cursor.execute("""
                                INSERT INTO votes (voter_id, candidate_id, voting_time)
                                VALUES (%(voter_id)s, %(candidate_id)s, %(voting_time)s)
                            """, vote)
                            processed_messages += 1
                            
                            if processed_messages % commit_interval == 0:
                                # Commit the transaction every commit_interval messages
                                conn.commit()
                                producer.produce(
                                    'votes_topic',
                                    key=vote['voter_id'],
                                    value=json.dumps(vote),
                                    on_delivery=delivery_report
                                )
                        except Exception as e:
                            print(f"An error occurred: {e}")
                        
            except Exception as e:
                print(f"An error occurred: {e}")
            # Commit any remaining messages
            conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
            
        finally:
            conn.close()
