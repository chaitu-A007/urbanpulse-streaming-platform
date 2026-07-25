import json
import math
import time
import logging
from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("FlinkIncidentDetector")

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

# Initialize Consumer reading from the 3 main streams
consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'flink_incident_detection_group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True
})

producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

consumer.subscribe(['urbanpulse.air_quality', 'urbanpulse.traffic_signals', 'urbanpulse.bus_gps'])

# -------------------------------------------------------------
# Keyed State Store (Simulated Flink ProcessFunction State)
# -------------------------------------------------------------
# Gridlock State: junction_id -> list of wait times [w1, w2, w3]
gridlock_state = {}

# Bus Bunching State: route_id -> dict of {bus_id: (lat, lon, first_bunch_timestamp)}
bus_positions = {}
bunching_tracker = {}

def haversine_distance(lat1, lon1, lat2, lon2):
    """ Calculate geodesic distance in meters between two lat/lon points """
    R = 6371000 # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def emit_incident(incident_type, severity, details):
    """ Outputs incident alerts directly to urbanpulse.incidents topic """
    payload = {
        "incident_type": incident_type,
        "severity": severity,
        "details": details,
        "timestamp": int(time.time() * 1000)
    }
    producer.produce(
        topic='urbanpulse.incidents',
        key=incident_type.encode('utf-8'),
        value=json.dumps(payload).encode('utf-8')
    )
    producer.poll(0)
    logger.warning(f"🚨 [INCIDENT DETECTED - {incident_type}] {details}")

# -------------------------------------------------------------
# Pattern Processors
# -------------------------------------------------------------
def process_aqi_event(data):
    """ Pattern (a): AQI Emergency (>300) """
    aqi = data.get('aqi')
    sensor_id = data.get('sensor_id')
    zone = data.get('zone')
    
    if aqi is not None and aqi > 300:
        emit_incident(
            incident_type="AQI_EMERGENCY",
            severity="CRITICAL",
            details={"sensor_id": sensor_id, "zone": zone, "aqi": aqi, "message": "AQI exceeded hazardous threshold (>300)"}
        )

def process_traffic_event(data):
    """ Pattern (b): Traffic Gridlock (avg_wait > 180s for 3 consecutive cycles) """
    junction_id = data.get('junction_id')
    zone = data.get('zone')
    avg_wait = data.get('avg_wait_sec', 0)

    if junction_id not in gridlock_state:
        gridlock_state[junction_id] = []

    # Maintain a sliding history of the last 3 signal cycles
    gridlock_state[junction_id].append(avg_wait)
    if len(gridlock_state[junction_id]) > 3:
        gridlock_state[junction_id].pop(0)

    # Check if last 3 consecutive cycles all exceed 180 seconds
    if len(gridlock_state[junction_id]) == 3 and all(w > 180 for w in gridlock_state[junction_id]):
        emit_incident(
            incident_type="TRAFFIC_GRIDLOCK",
            severity="HIGH",
            details={
                "junction_id": junction_id,
                "zone": zone,
                "consecutive_wait_times": gridlock_state[junction_id],
                "message": "Junction gridlock detected! Wait time > 180s for 3 consecutive cycles."
            }
        )
        # Reset state to avoid spamming alerts every event
        gridlock_state[junction_id] = []

def process_bus_gps_event(data):
    """ Pattern (c): Bus Bunching (2 buses on same route <= 200m for > 5 mins) """
    route_id = data.get('route_id')
    bus_id = data.get('bus_id')
    lat = data.get('lat')
    lon = data.get('lon')
    curr_time = time.time()

    if route_id not in bus_positions:
        bus_positions[route_id] = {}

    bus_positions[route_id][bus_id] = (lat, lon, curr_time)

    # Cross-compare all active buses on this route
    buses_on_route = list(bus_positions[route_id].items())
    for i in range(len(buses_on_route)):
        for j in range(i + 1, len(buses_on_route)):
            b1_id, (b1_lat, b1_lon, _) = buses_on_route[i]
            b2_id, (b2_lat, b2_lon, _) = buses_on_route[j]

            dist = haversine_distance(b1_lat, b1_lon, b2_lat, b2_lon)
            bunch_key = f"{route_id}_{min(b1_id, b2_id)}_{max(b1_id, b2_id)}"

            if dist <= 200.0: # Within 200 meters
                if bunch_key not in bunching_tracker:
                    bunching_tracker[bunch_key] = curr_time
                elif curr_time - bunching_tracker[bunch_key] >= 300: # 5 minutes (300 seconds)
                    emit_incident(
                        incident_type="BUS_BUNCHING",
                        severity="MEDIUM",
                        details={
                            "route_id": route_id,
                            "bus_1": b1_id,
                            "bus_2": b2_id,
                            "distance_meters": round(dist, 2),
                            "duration_seconds": int(curr_time - bunching_tracker[bunch_key])
                        }
                    )
                    # Reset timer after alert
                    bunching_tracker[bunch_key] = curr_time
            else:
                bunching_tracker.pop(bunch_key, None)

# -------------------------------------------------------------
# Main Streaming Loop
# -------------------------------------------------------------
def run_flink_processor():
    logger.info("Starting PyFlink Stream Incident Detector...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue

            topic = msg.topic()
            data = json.loads(msg.value().decode('utf-8'))

            if topic == 'urbanpulse.air_quality':
                process_aqi_event(data)
            elif topic == 'urbanpulse.traffic_signals':
                process_traffic_event(data)
            elif topic == 'urbanpulse.bus_gps':
                process_bus_gps_event(data)

    except KeyboardInterrupt:
        logger.info("Stopping Flink Incident Detector...")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    run_flink_processor()