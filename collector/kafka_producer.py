import json
from kafka import KafkaProducer

class KafkaBufferedProducer:
    """Produces messages to Kafka using micro-batching and event-driven deadband."""
    def __init__(self, bootstrap_server, topic, buffer_size):
        self.topic = topic
        self.buffer_size = buffer_size
        self.buffer = []
        self.batch_count = 0
        
        print(f"[KAFKA] Connecting to broker at {bootstrap_server}...")
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("[KAFKA] Connected successfully.")

    def add(self, payload):
        self.buffer.append(payload)
        # Flush condition 1: Deadband (lap crossed)
        # Flush condition 2: Micro-batch buffer full
        if payload.get('lap_crossed', False) or len(self.buffer) >= self.buffer_size:
            self.flush()

    def flush(self):
        if not self.buffer:
            return
        
        self.batch_count += 1
        # Send the entire buffer array as a single JSON message
        self.producer.send(self.topic, value=self.buffer)
        
        # Log the action
        is_deadband = any(p.get('lap_crossed') for p in self.buffer)
        flush_type = "DEADBAND" if is_deadband else "BATCH"
        print(f"[KAFKA] {flush_type} Flush #{self.batch_count} sent ({len(self.buffer)} samples).")
        
        self.buffer = []

    def close(self):
        self.flush()
        self.producer.flush()
        self.producer.close()
        print("[KAFKA] Producer closed.")


class DryRunProducer:
    """Mock producer that prints batches to the console instead of sending to Kafka."""
    def __init__(self, buffer_size):
        self.buffer_size = buffer_size
        self.buffer = []
        self.batch_count = 0
        print("[DRY-RUN] Initialized. Data will be printed to console, not sent to Kafka.")

    def add(self, payload):
        self.buffer.append(payload)
        if payload.get('lap_crossed', False) or len(self.buffer) >= self.buffer_size:
            self.flush()

    def flush(self):
        if not self.buffer:
            return
            
        self.batch_count += 1
        is_deadband = any(p.get('lap_crossed') for p in self.buffer)
        flush_type = "DEADBAND" if is_deadband else "BATCH"
        
        print(f"\n--- [DRY-RUN] {flush_type} Flush #{self.batch_count} ---")
        print(f"Total samples in this batch: {len(self.buffer)}")
        if is_deadband:
            print("Reason: Lap crossing detected!")
        # Print just the first and last sample of the batch to keep console clean
        print(f"First sample: {self.buffer[0]}")
        if len(self.buffer) > 1:
            print(f"Last sample:  {self.buffer[-1]}")
        print("----------------------------------------")
        
        self.buffer = []

    def close(self):
        self.flush()
        print("[DRY-RUN] Producer closed.")
