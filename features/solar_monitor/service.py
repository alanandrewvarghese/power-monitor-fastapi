import logging
import serial
from datetime import datetime
from queue import Queue, Empty
import threading
import time
from .modbus import read_holding_registers, parse_pzem_data
from common.database import db_connection, log_to_db_production
from config import get_solar_config

# Background thread to capture solar data and put it in a queue
def capture_solar_data(data_queue, stop_event=None):
    config = get_solar_config()
    ser = None
    try:
        ser = serial.Serial(config['serial_port'], config['baud_rate'], timeout=config['serial_timeout'])
        logging.info(f"Connected to serial port: {config['serial_port']}")
        while not (stop_event and stop_event.is_set()):
            try:
                registers = read_holding_registers(ser, 0x00, 8, config)
                if registers:
                    data = parse_pzem_data(registers, config)
                    if data:
                        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        data_with_timestamp = (timestamp, data['voltage'], data['current'], data['power'], data['energy'])
                        data_queue.put(data_with_timestamp)
                        logging.info(f"Captured data (Solar): {timestamp}, Voltage: {data['voltage']}, Current: {data['current']}, Power: {data['power']}, Energy: {data['energy']}")
                else:
                    logging.warning("No data received from PZEM device")
            except Exception as e:
                logging.error(f"Error in data capture: {e}")
            time.sleep(1)
    finally:
        if ser and ser.is_open:
            ser.close()
            logging.info("Serial port closed.")

# Background thread to transfer solar data from queue to database
def transfer_solar_to_database(data_queue, stop_event=None):
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
                        log_to_db_production(connection, batch)
                        logging.info(f"Transferred {len(batch)} records to the database.")
            except Exception as e:
                logging.error(f"Database transfer error: {e}")
                for record in batch:
                    data_queue.put(record)
        time.sleep(30)
