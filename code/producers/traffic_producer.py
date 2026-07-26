import json
import time
import random
from confluent_kafka import Producer

# Kafka broker setup (adjust if using a single port like 'localhost:9092')
conf = {'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094'}
producer = Producer(conf)

topic = 'urbanpulse.traffic_signals'
signal_ids = ['SIG-101', 'SIG-102', 'SIG-103', 'SIG-104', 'SIG-105']
statuses = ['GREEN', 'YELLOW', 'RED']

print("🚀 Traffic Signals Producer started. Streaming events...")

try:
    while True:
        payload = {
            'signal_id': random.choice(signal_ids),
            'status': random.choice(statuses),
            'vehicle_count': random.randint(5, 50),
            'timestamp': time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        # Send message to Kafka
        producer.produce(
            topic, 
            key=payload['signal_id'], 
            value=json.dumps(payload).encode('utf-8')
        )
        producer.poll(0)
        
        # Fast streaming rate to quickly generate lag in slow consumers
        time.sleep(0.05) 
        
except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.flush()