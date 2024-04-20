import datetime
import random
import json
from database import connect_to_database
from confluent_kafka import Consumer, KafkaError, SerializingProducer

# Kafka configuration
KAFKA_CONF = {
    "bootstrap.servers": "localhost:9092"
}

# Kafka topic names
VOTERS_TOPIC = 'voters_topic'
VOTES_TOPIC = 'votes_topic'

# Kafka group ID
GROUP_ID = 'voting-group'

# Commit interval
COMMIT_INTERVAL = 10

# Kafka consumer instance
consumer = Consumer(KAFKA_CONF | {
    'group.id': GROUP_ID,
    'auto.offset': 'earliest',
    'enable.auto.commit': False
})

# Kafka producer instance
producer = SerializingProducer(KAFKA_CONF)


def delivery_report(err, msg):
    """Delivery report callback function."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def fetch_candidates(cursor):
    """Fetch candidates data from the database."""
    cursor.execute("""
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
    return [candidate[0] for candidate in cursor.fetchall()]


def process_vote(voter, candidates, cursor):
    """Process a vote."""
    chosen_candidate = random.choice(candidates)
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
        return vote
    except Exception as e:
        print(f"An error occurred: {e}")


def main():
    """Main function to run the voting system."""
    conn = connect_to_database()

    if conn:
        try:
            cursor = conn.cursor()
            candidates = fetch_candidates(cursor)

            if len(candidates) == 0:
                raise Exception("No candidates found in the database.")

            consumer.subscribe([VOTERS_TOPIC])
            processed_messages = 0

            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break
                else:
                    voter = json.loads(msg.value().decode('utf-8'))
                    vote = process_vote(voter, candidates, cursor)
                    processed_messages += 1

                    if processed_messages % COMMIT_INTERVAL == 0:
                        # Commit the transaction every COMMIT_INTERVAL messages
                        conn.commit()

                        # Produce the vote to Kafka
                        producer.produce(
                            VOTES_TOPIC,
                            key=vote['voter_id'],
                            value=json.dumps(vote),
                            on_delivery=delivery_report
                        )

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            conn.close()
            # Flush any remaining messages to Kafka
            producer.flush()


if __name__ == "__main__":
    main()
