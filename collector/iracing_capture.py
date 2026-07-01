import time
import irsdk

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
