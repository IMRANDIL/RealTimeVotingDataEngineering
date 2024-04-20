import datetime
import random
from database import connect_to_database
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
import simplejson as json
conf = {
    "bootstrap.servers": "localhost:9092"
}

consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf)


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
                        vote = voter | chosen_candidate | {
                            'voting_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                            'vote': 1   
                        }
                        
                        try:
                            print(f"User {vote['voter_id']} is voting for candidate: {vote['candidate_id']}")
                            
                        except Exception as e:
                            print(f"An error occurred: {e}")
                        
            except Exception as e:
                print(f"An error occurred: {e}")
            
        except Exception as e:
            print(f"An error occurred: {e}")
            
        finally:
            conn.close()
