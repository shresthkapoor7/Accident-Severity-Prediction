"""
Kafka Consumer - Consumes accident data from Kafka/Redpanda and streams to dashboard
Uses kafka-python for simple consumption (no Spark required)
"""

import json
import sys
from collections import deque
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import threading
import time

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Redpanda default port

# Storage for streaming predictions
predictions_queue = deque(maxlen=100)
alerts_queue = deque(maxlen=50)

# State
consumer = None  # KafkaConsumer object - will be created when start_consuming is called
consumer_thread = None
consuming_active = False
current_state = None  # None = all states, "NY" = New York only
_consumer_group_id = 'accident-dashboard-consumer'  # Default group ID


def get_topic_for_state(state_code):
    """Get Kafka topic for a state (None = all)"""
    if state_code is None:
        return "accident-all"
    return f"accident-{state_code.lower()}"


def process_message(message):
    """Process a single Kafka message and add to predictions queue"""
    try:
        # Parse JSON message
        accident_data = json.loads(message.value.decode('utf-8'))
        
        # Add processed timestamp
        accident_data['processed_at'] = datetime.now().isoformat()
        
        # Add to predictions queue
        predictions_queue.append(accident_data)
        
        # Check if high severity (we'll add prediction later)
        # For now, just add to alerts if adverse weather
        if accident_data.get('is_adverse_weather', False):
            alerts_queue.append({
                **accident_data,
                "alert_level": "WARNING"
            })
        
        print(f"  [{accident_data.get('state', '?')}] {accident_data.get('id', '?')}: "
              f"{accident_data.get('city', '?')} | {accident_data.get('weather_condition', '?')}")
        
    except Exception as e:
        print(f"Error processing message: {e}")


def create_consumer(topic, group_id):
    """Create a KafkaConsumer object"""
    global consumer
    
    try:
        print(f"[CONSUMER] Creating Kafka consumer for topic: {topic}")
        print(f"[CONSUMER] Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"[CONSUMER] Consumer group ID: {group_id}")
        
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: m,  # We'll decode manually
            auto_offset_reset='latest',  # Start from latest (only new messages after consumer starts)
            enable_auto_commit=True,
            consumer_timeout_ms=2000,  # Timeout for polling (increased)
            group_id=group_id,
            # Redpanda/Kafka connection settings
            request_timeout_ms=30000,
            api_version=(0, 10, 1),
            # Additional settings for better message consumption
            fetch_min_bytes=1,
            fetch_max_wait_ms=500
        )
        
        print(f" Consumer object created and connected to Kafka/Redpanda at {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"✓ Subscribed to topic: {topic}")
        print(f"✓ Consumer Group ID: {group_id}")
        print(f"✓ Auto offset reset: latest (only new messages)")
        return consumer
        
    except Exception as e:
        print(f"Failed to create consumer object: {e}")
        print(f"  Make sure Kafka/Redpanda is running at {KAFKA_BOOTSTRAP_SERVERS}")
        consumer = None
        return None


def consumer_worker():
    """Background worker that consumes from Kafka"""
    global consumer, consuming_active, current_state, _consumer_group_id
    
    # Force flush prints to ensure they appear in logs
    sys.stdout.flush()
    
    topic = get_topic_for_state(current_state)
    print(f"[CONSUMER WORKER] Starting worker thread for topic: {topic}", flush=True)
    sys.stdout.flush()
    
    # Consumer object should already be created by start_consuming()
    if consumer is None:
        print("[CONSUMER WORKER] ERROR: Consumer object is None! Creating now...", flush=True)
        consumer = create_consumer(topic, _consumer_group_id)
        if consumer is None:
            print("[CONSUMER WORKER] Failed to create consumer object, worker exiting", flush=True)
            sys.stdout.flush()
            return
    
    print(f"[CONSUMER WORKER] Consumer object exists: {consumer is not None}", flush=True)
    print(f"[CONSUMER WORKER] Starting to poll for messages...", flush=True)
    sys.stdout.flush()
    
    message_count = 0
    poll_count = 0
    while consuming_active:
        try:
            # Poll for messages (non-blocking with timeout)
            message_pack = consumer.poll(timeout_ms=2000)
            poll_count += 1
            
            if message_pack:
                for topic_partition, messages in message_pack.items():
                    print(f"[CONSUMER WORKER] Received {len(messages)} messages from partition {topic_partition.partition}", flush=True)
                    for message in messages:
                        process_message(message)
                        message_count += 1
            else:
                # No messages - log every 10 polls to show it's alive
                if poll_count % 10 == 0:
                    print(f"[CONSUMER WORKER] [Poll {poll_count}] Waiting for messages on topic '{topic}'...", flush=True)
            
        except KafkaError as e:
            print(f"Kafka error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(1)
        except Exception as e:
            print(f"Error in consumer worker: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(1)
    
    print(f"Consumer processed {message_count} total messages")
    print("Consumer worker stopped")


def start_consuming(state_code=None, reset_group=False):
    """Start consuming from Kafka - creates consumer object and starts worker thread"""
    global consuming_active, consumer_thread, current_state, consumer, _consumer_group_id
    
    if consuming_active:
        stop_consuming()
        time.sleep(0.5)  # Give old consumer time to close
    
    current_state = state_code
    consuming_active = True
    
    # Clear old data
    predictions_queue.clear()
    alerts_queue.clear()
    
    # Always use a unique group ID to avoid offset issues
    # This ensures we get new messages each time we start
    _consumer_group_id = f'accident-dashboard-consumer-{int(time.time())}'
    print(f"[CONSUMER] Using consumer group ID: {_consumer_group_id}")
    
    topic = get_topic_for_state(state_code)
    
    # Create the consumer object BEFORE starting the thread
    consumer = create_consumer(topic, _consumer_group_id)
    
    if consumer is None:
        print("[CONSUMER] Failed to create consumer object")
        consuming_active = False
        return False
    
    # Start consumer thread (consumer object is already created)
    consumer_thread = threading.Thread(target=consumer_worker, daemon=True)
    consumer_thread.start()
    
    # Give it a moment to start
    time.sleep(0.5)
    
    state_name = state_code if state_code else "All US"
    print(f"Started consuming for: {state_name}")
    return True


def stop_consuming():
    """Stop consuming from Kafka and close consumer object"""
    global consuming_active, consumer
    
    consuming_active = False
    
    if consumer:
        try:
            print("[CONSUMER] Closing consumer object...")
            consumer.close()
            consumer = None
            print("[CONSUMER] Consumer object closed")
        except Exception as e:
            print(f"[CONSUMER] Error closing consumer: {e}")
            consumer = None
    
    print("Stopped consuming")


def switch_state(state_code):
    """Switch to a different state's topic"""
    if state_code == current_state:
        return True
    return start_consuming(state_code)


def get_latest_predictions(count=50):
    """Get latest predictions from queue"""
    return list(predictions_queue)[-count:]


def get_latest_alerts(count=10):
    """Get latest alerts from queue"""
    return list(alerts_queue)[-count:]


def is_consuming():
    """Check if consumer is active"""
    return consuming_active


def get_current_state():
    """Get current state filter"""
    return current_state


if __name__ == "__main__":
    print("=" * 60)
    print("Kafka Consumer - Accident Dashboard")
    print("=" * 60)
    
    if start_consuming(None):  # Start with all states
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            stop_consuming()

