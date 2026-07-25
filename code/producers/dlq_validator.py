import json
import time
import logging
from collections import Counter
from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("DLQValidator")

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'dlq_validation_group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True
})

producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

consumer.subscribe(['urbanpulse.air_quality', 'urbanpulse.bus_gps'])

error_counter = Counter()
total_scanned = 0

def validate_event(topic, record):
    """ Enforces business rules and identifies errors """
    if topic == 'urbanpulse.air_quality':
        if record.get('aqi') is None:
            return "NULL_AQI_VALUE"
        if not (0 <= record.get('aqi') <= 500):
            return "OUT_OF_RANGE_AQI"
            
    elif topic == 'urbanpulse.bus_gps':
        lat, lon = record.get('lat', 0), record.get('lon', 0)
        # Bounding box for MetroConnect region (18.5-19.5 N, 72.5-73.5 E)
        if not (18.5 <= lat <= 19.5 and 72.5 <= lon <= 73.5):
            return "IMPOSSIBLE_GPS_COORDINATES"
            
    return None

def run_dlq_processor():
    global total_scanned
    logger.info("Starting DLQ Validation Consumer Service...")
    start_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue

            total_scanned += 1
            topic = msg.topic()
            payload = json.loads(msg.value().decode('utf-8'))

            error_reason = validate_event(topic, payload)

            if error_reason:
                error_counter[error_reason] += 1
                dlq_payload = {
                    "original_topic": topic,
                    "error_reason": error_reason,
                    "failed_payload": payload,
                    "failed_at": int(time.time() * 1000)
                }
                producer.produce(
                    topic='urbanpulse.dlq',
                    value=json.dumps(dlq_payload).encode('utf-8')
                )
                producer.poll(0)
                logger.warning(f"[DLQ ROUTED] Reason: {error_reason} | Topic: {topic}")

            # Print stats summary every 30 seconds
            if time.time() - start_time >= 30:
                logger.info(f"--- 30-Sec DLQ Summary: Scanned {total_scanned} records. Errors: {dict(error_counter)} ---")
                start_time = time.time()

    except KeyboardInterrupt:
        logger.info("Stopping DLQ Validator...")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    run_dlq_processor()