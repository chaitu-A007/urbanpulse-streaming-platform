import json
import csv
import os
import logging
from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("RouteEnrichmentApp")

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

# 1. Load Static Route Schedule Lookup (Acts as a KTable state store)
ROUTE_SCHEDULE_KTABLE = {}

def load_route_schedule():
    csv_path = os.path.join(os.path.dirname(__file__), '../data/route_schedule.csv')
    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found at {csv_path}")
        return
    
    with open(csv_path, mode='r') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            ROUTE_SCHEDULE_KTABLE[row['route_id']] = {
                "route_name": row['route_name'],
                "terminal": row['terminal'],
                "scheduled_arrival_time": row['scheduled_arrival_time']
            }
    logger.info(f"Loaded {len(ROUTE_SCHEDULE_KTABLE)} routes into local stream state (KTable).")

# 2. Setup Kafka Consumer & Producer
consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'route_enrichment_stream_group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True
})

producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

consumer.subscribe(['urbanpulse.bus_gps'])

def run_stream_enrichment():
    load_route_schedule()
    logger.info("Starting Real-Time Route Enrichment Engine...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue

            # Deserialize incoming GPS event
            gps_event = json.loads(msg.value().decode('utf-8'))
            route_id = gps_event.get('route_id')

            # Stream-Table Join: Enrich event with static KTable lookup
            if route_id in ROUTE_SCHEDULE_KTABLE:
                schedule_meta = ROUTE_SCHEDULE_KTABLE[route_id]
                gps_event['route_name'] = schedule_meta['route_name']
                gps_event['terminal'] = schedule_meta['terminal']
                gps_event['scheduled_arrival_time'] = schedule_meta['scheduled_arrival_time']

            # Produce enriched message to destination topic
            producer.produce(
                topic='urbanpulse.bus_gps_enriched',
                key=msg.key(),
                value=json.dumps(gps_event).encode('utf-8')
            )
            producer.poll(0)

            logger.info(f"[ENRICHED] Bus {gps_event.get('bus_id')} on {gps_event.get('route_name')} -> Terminal: {gps_event.get('terminal')}")

    except KeyboardInterrupt:
        logger.info("Closing Route Enrichment Stream Application...")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    run_stream_enrichment()