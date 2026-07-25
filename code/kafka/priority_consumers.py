import time
import json
import threading
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'

# -------------------------------------------------------------
# 1. HIGH_PRIORITY Consumer (Real-Time Signal Control System)
# -------------------------------------------------------------
def run_high_priority_consumer():
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'signal_control_high_priority_group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    consumer = Consumer(conf)
    consumer.subscribe(['urbanpulse.traffic_signals'])
    
    print("[HIGH_PRIORITY] Control System Consumer initialized. Target Lag: ~0")
    try:
        while True:
            msg = consumer.poll(timeout=0.1)
            if msg is None:
                continue
            if msg.error():
                continue
            
            # Fast real-time processing loop (Zero sleep)
            data = json.loads(msg.value().decode('utf-8'))
            # Fast action: Evaluate adaptive signal logic...
    except Exception as e:
        print(f"[HIGH_PRIORITY] Error: {e}")
    finally:
        consumer.close()

# -------------------------------------------------------------
# 2. STANDARD_PRIORITY Consumer (Analytics Dashboard - Throttled)
# -------------------------------------------------------------
def run_standard_priority_consumer(consumer_id):
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'analytics_dashboard_standard_group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    consumer = Consumer(conf)
    consumer.subscribe(['urbanpulse.traffic_signals'])
    
    print(f"[STANDARD_PRIORITY] Analytics Worker {consumer_id} started.")
    try:
        while True:
            msg = consumer.poll(timeout=0.1)
            if msg is None:
                continue
            if msg.error():
                continue
            
            # Simulated slow operational/database writing delay
            time.sleep(0.2) 
            data = json.loads(msg.value().decode('utf-8'))
    except Exception as e:
        print(f"[STANDARD_PRIORITY {consumer_id}] Error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    # Start 1 HIGH_PRIORITY consumer thread
    t_high = threading.Thread(target=run_high_priority_consumer)
    t_high.daemon = True
    t_high.start()

    # Start 3 STANDARD_PRIORITY consumer threads
    for i in range(3):
        t_std = threading.Thread(target=run_standard_priority_consumer, args=(i+1,))
        t_std.daemon = True
        t_std.start()

    t_high.join()