import configparser
import os
import sys
import json
import time
import argparse
import irsdk
from kafka import KafkaProducer

def get_base_path():
    """Returns the base path of the executable or script."""
    if getattr(sys, 'frozen', False):
        # Running as a packaged .exe
        return os.path.dirname(sys.executable)
    else:
        # Running as a .py script
        return os.path.dirname(os.path.abspath(__file__))

def load_config():
    """Loads and parses the config.ini file."""
    base_path = get_base_path()
    config_path = os.path.join(base_path, 'config.ini')
    
    if not os.path.exists(config_path):
        print(f"[ERROR] Configuration file not found at: {config_path}")
        sys.exit(1)
        
    config = configparser.ConfigParser()
    config.read(config_path)
    
    # Extract parameters into a dictionary
    try:
        return {
            'bootstrap_server': config.get('kafka', 'bootstrap_server'),
            'topic': config.get('kafka', 'topic'),
            'dry_run': config.getboolean('collector', 'dry_run', fallback=False),
            'sample_rate_hz': config.getint('collector', 'sample_rate_hz'),
            'buffer_size': config.getint('collector', 'buffer_size')
        }
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        print(f"[ERROR] Invalid config.ini: {e}")
        sys.exit(1)

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


def live_generator():
    """Connects to iRacing, captures telemetry, and yields 10Hz downsampled payloads."""
    ir = irsdk.IRSDK()
    ir.startup()
    
    prev_lap = 0
    frame_counter = 0
    
    print("[LIVE] Waiting for iRacing connection...")
    
    while True:
        if ir.is_connected():
            frame_counter += 1
            
            # Downsampling: iRacing updates at 60Hz. 
            # Processing 1 in every 6 frames gives us exactly 10Hz.
            if frame_counter % 6 != 0:
                time.sleep(1 / 60)
                continue
                
            current_lap = ir['Lap']
            
            payload = {
                'session_time': ir['SessionTime'],
                'lap': current_lap,
                'fuel': ir['FuelLevel'],
                'steering': ir['SteeringWheelAngle'],
                'throttle': ir['Throttle'],
                'brake': ir['Brake'],
                'temp_lf': ir['LFtempCL'],
                'temp_rf': ir['RFtempCL'],
                'temp_lr': ir['LRtempCL'],
                'temp_rr': ir['RRtempCL'],
                'lap_dist_pct': ir['LapDistPct'],
            }
            
            # State Change Detection (Lap Crossing)
            if current_lap > prev_lap:
                lap_time = ir['LapLastLapTime']
                payload['lap_time'] = lap_time
                payload['lap_crossed'] = True
                
                # Only print lap info if we have a valid previous lap to avoid 
                # printing a massive time when joining a session midway
                if prev_lap > 0:
                    print(f"[LIVE] Lap {prev_lap} completed! Time: {lap_time:.3f}s")
                
                prev_lap = current_lap
                
            yield payload
            
        else:
            # If simulator is closed, wait and retry
            print("[LIVE] iRacing not running or disconnected. Retrying in 5 seconds...")
            ir.startup()
            time.sleep(5)
            
        # Base sleep cycle (60Hz polling rate)
        time.sleep(1 / 60)


def parse_args():
    parser = argparse.ArgumentParser(description="iRacing Telemetry Collector")
    parser.add_argument('--dry-run', action='store_true', help="Override config.ini and force dry-run mode")
    return parser.parse_args()

def main():
    args = parse_args()
    config = load_config()
    
    # CLI argument overrides the config file if used
    is_dry_run = args.dry_run or config['dry_run']
    
    print("[iRacing Collector] Configuration loaded:")
    print(f"  Kafka Server:     {config['bootstrap_server']}")
    print(f"  Kafka Topic:      {config['topic']}")
    print(f"  Dry Run Mode:     {is_dry_run}")
    print(f"  Sample Rate:      {config['sample_rate_hz']} Hz")
    print(f"  Buffer Size:      {config['buffer_size']}")
    
    # 1. Initialize Producer
    if is_dry_run:
        producer = DryRunProducer(config['buffer_size'])
    else:
        try:
            producer = KafkaBufferedProducer(
                config['bootstrap_server'],
                config['topic'],
                config['buffer_size']
            )
        except Exception as e:
            print(f"[ERROR] Failed to connect to Kafka: {e}")
            sys.exit(1)
            
    print("\n[iRacing Collector] Initialization complete. Starting live capture...")
    
    # 2. Run Capture Loop
    try:
        for payload in live_generator():
            producer.add(payload)
    except KeyboardInterrupt:
        print("\n[COLLECTOR] Stopped by user (Ctrl+C).")
    finally:
        producer.close()

if __name__ == '__main__':
    main()
