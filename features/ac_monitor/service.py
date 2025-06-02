import logging
import serial
from datetime import datetime
from queue import Queue, Empty
import threading
import time
import subprocess
from .modbus import read_holding_registers, parse_pzem_data
from common.database import db_connection, log_to_db_consumption
from config import get_ac_config

# Helper function to close active serial connections
def close_active_serial_connections(port):
    """Attempt to close any active connections to the specified serial port."""
    try:
        result = subprocess.run(['lsof', port], capture_output=True, text=True)
        if result.stdout:
            lines = result.stdout.splitlines()
            for line in lines[1:]:  # Skip header
                pid = line.split()[1]  # Extract PID
                logging.info(f"Terminating process {pid} using {port}")
                subprocess.run(['kill', pid])  # Terminate process gracefully
        else:
            logging.info(f"No active processes found using {port}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error checking/closing processes on {port}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while closing connections on {port}: {e}")

# Background thread to capture AC data and put it in a queue
def capture_ac_data(data_queue, stop_event=None, max_retries=3, retry_delay=2):
    config = get_ac_config()
    ser = None
    retry_count = 0
    
    while not (stop_event and stop_event.is_set()) and retry_count <= max_retries:
        try:
            ser = serial.Serial(config['serial_port'], config['baud_rate'], timeout=config['serial_timeout'])
            logging.info(f"Connected to serial port: {config['serial_port']}")
            base_interval = 1
            while not (stop_event and stop_event.is_set()):
                try:
                    registers = read_holding_registers(ser, 0x00, 10, config)
                    if registers:
                        data = parse_pzem_data(registers, config)
                        if data:
                            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            data_with_timestamp = (timestamp, data['voltage'], data['current'], data['power'], data['energy'], data['frequency'], data['power_factor'])
                            data_queue.put(data_with_timestamp)
                            logging.info(f"Captured data (AC): {timestamp}, Voltage: {data['voltage']}, Current: {data['current']}, Power: {data['power']}, Energy: {data['energy']}, Frequency: {data['frequency']}, Power Factor: {data['power_factor']}")
                    else:
                        logging.warning("No data received from PZEM device")
                except Exception as e:
                    logging.error(f"Error in data capture: {e}")
                queue_size = data_queue.qsize()
                sleep_time = max(base_interval - (queue_size / 1000), 0.1)
                time.sleep(sleep_time)
        
        except (serial.SerialException, OSError) as e:
            logging.error(f"Failed to open serial port {config['serial_port']}: {e}")
            retry_count += 1
            if retry_count > max_retries:
                logging.error(f"Max retries ({max_retries}) reached. Giving up on {config['serial_port']}")
                break
            logging.info(f"Attempting to close active connections on {config['serial_port']} (Retry {retry_count}/{max_retries})")
            if ser and ser.is_open:
                ser.close()
                logging.info("Closed existing serial connection.")
            close_active_serial_connections(config['serial_port'])
            logging.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
        
        finally:
            if ser and ser.is_open:
                ser.close()
                logging.info("Serial port closed.")
    
    if retry_count > max_retries:
        logging.error("Exiting capture_ac_data due to repeated serial port failures.")

# Background thread to transfer AC data from queue to database
def transfer_ac_to_database(data_queue, stop_event=None):
    base_interval = 30
    while not (stop_event and stop_event.is_set()):
        batch = []
        try:
            for _ in range(50):
                batch.append(data_queue.get_nowait())
        except Empty:
            pass
        if batch:
            try:
                with db_connection() as connection:
                    if connection:
                        log_to_db_consumption(connection, batch)
                        logging.info(f"Transferred {len(batch)} records to the database.")
            except Exception as e:
                logging.error(f"Database transfer error: {e}")
                for record in batch:
                    data_queue.put(record)
        queue_size = data_queue.qsize()
        sleep_time = max(base_interval - (queue_size / 100), 5)
        time.sleep(sleep_time)

# Optional: function to display real-time data (for CLI/debug)
def display_ac_realtime_data(data_queue, stop_event=None):
    while not (stop_event and stop_event.is_set()):
        try:
            data = data_queue.get_nowait()
            timestamp, voltage, current, power, energy, frequency, power_factor = data
            print(f"Timestamp: {timestamp}, Voltage: {voltage} V, Current: {current} A, Power: {power} W, Energy: {energy/1000} kWh, Frequency: {frequency} Hz, PF: {power_factor}")
        except Empty:
            time.sleep(0.1)
        except Exception as e:
            logging.error(f"Error in display_ac_realtime_data: {e}")