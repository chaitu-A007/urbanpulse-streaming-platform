import json
import random
import time
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer

# Setup logging for monitoring delivery and sensor failures
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger("UrbanPulseProducers")

# -------------------------------------------------------------
# Kafka Producer Configuration
# -------------------------------------------------------------
producer_config = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'acks': 'all',
    'enable.idempotence': True,
    'retries': 5,
    'retry.backoff.ms': 100,
    'max.in.flight.requests.per.connection': 5,
    'compression.type': 'snappy'
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    """ Async delivery callback """
    if err is not None:
        logger.error(f"Delivery failed for key {msg.key()}: {err}")

def get_current_timestamp():
    """ Returns standard ISO-8601 formatted timestamp string """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# -------------------------------------------------------------
# 1. Bus GPS Producer
# -------------------------------------------------------------
def produce_bus_gps():
    routes = ["ROUTE_101", "ROUTE_202", "ROUTE_303", "ROUTE_404"]
    bus_id = f"BUS_{random.randint(1000, 9999)}"
    route_id = random.choice(routes)
    
    payload = {
        "bus_id": bus_id,
        "route_id": route_id,
        "lat": 19.0760 + random.uniform(-0.05, 0.05),
        "lon": 72.8777 + random.uniform(-0.05, 0.05),
        "speed_kmh": round(random.uniform(10.0, 60.0), 2),
        "occupancy_pct": random.randint(10, 95),
        "timestamp": get_current_timestamp()
    }
    
    producer.produce(
        topic='urbanpulse.bus_gps',
        key=route_id.encode('utf-8'),
        value=json.dumps(payload).encode('utf-8'),
        on_delivery=delivery_report
    )

# -------------------------------------------------------------
# 2. Air Quality Producer
# -------------------------------------------------------------
def produce_air_quality():
    sensor_id = f"AQI_SENS_{random.randint(100, 150)}"
    zone = f"ZONE_{random.randint(1, 10)}"
    
    is_faulty = random.random() < 0.05
    if is_faulty:
        aqi_val = None
        pm25_val = None
    else:
        aqi_val = random.randint(50, 350)
        pm25_val = round(random.uniform(15.0, 180.0), 2)

    payload = {
        "sensor_id": sensor_id,
        "zone": zone,
        "pm25": pm25_val,
        "pm10": round(random.uniform(30.0, 250.0), 2),
        "no2": round(random.uniform(10.0, 80.0), 2),
        "aqi": aqi_val,
        "timestamp": get_current_timestamp()
    }

    producer.produce(
        topic='urbanpulse.air_quality',
        key=sensor_id.encode('utf-8'),
        value=json.dumps(payload).encode('utf-8'),
        on_delivery=delivery_report
    )

# -------------------------------------------------------------
# 3. Smart Meter Producer
# -------------------------------------------------------------
def produce_smart_meters():
    meter_id = f"MTR_{random.randint(1000, 5000)}"
    ward_id = f"W{random.randint(1, 5)}"  # W1 to W5
    
    payload = {
        "meter_id": meter_id,
        "ward_id": ward_id,
        "kwh_reading": round(random.uniform(0.5, 15.0), 2),
        "voltage": round(random.uniform(210.0, 240.0), 1),
        "power_factor": round(random.uniform(0.85, 0.99), 2),
        "timestamp": get_current_timestamp()
    }

    producer.produce(
        topic='urbanpulse.smart_meters',
        key=ward_id.encode('utf-8'),
        value=json.dumps(payload).encode('utf-8'),
        on_delivery=delivery_report
    )

# -------------------------------------------------------------
# Main Execution Loop
# -------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Starting UrbanPulse Kafka Producers...")
    logger.info("Producing to topics: urbanpulse.bus_gps, urbanpulse.air_quality, urbanpulse.smart_meters")
    
    event_count = 0
    try:
        while True:
            produce_bus_gps()
            produce_air_quality()
            produce_smart_meters()
            
            producer.poll(0)
            
            event_count += 3
            if event_count % 90 == 0:
                logger.info(f"Successfully generated and dispatched {event_count} telemetry events...")
                
            time.sleep(0.05)
            
    except KeyboardInterrupt:
        logger.info("Stopping producer stream. Flushing pending messages...")
        producer.flush()
        logger.info("Producer shutdown cleanly.")