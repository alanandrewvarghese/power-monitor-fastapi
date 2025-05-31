import struct
import serial
import logging
from config import get_ac_config, PRECISION

# Modbus function codes
READ_HOLDING_REGISTERS = 0x04

# Precomputed CRC16 table for faster calculation
CRC16_TABLE = [0x0000, 0xA001] + [0] * 254
for i in range(1, 256):
    crc = i
    for _ in range(8):
        if crc & 0x0001:
            crc = (crc >> 1) ^ 0xA001
        else:
            crc >>= 1
    CRC16_TABLE[i] = crc

# Calculate CRC for Modbus frames
def calculate_crc(data):
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        crc = (crc >> 8) ^ CRC16_TABLE[crc & 0xFF]
    return struct.pack('<H', crc)

# Send a Modbus request and read the response
def send_modbus_request(ser, function_code, register_address, num_registers, config=None):
    if config is None:
        config = get_ac_config()
    try:
        command = struct.pack('>BBHH', config['slave_address'], function_code, register_address, num_registers)
        command += calculate_crc(command)
        ser.write(command)
        response_length = 5 + 2 * num_registers
        response = ser.read(response_length)
        if len(response) < response_length:
            logging.warning("Incomplete response received")
            return None
        if calculate_crc(response[:-2]) != response[-2:]:
            logging.warning("CRC mismatch in response")
            return None
        return response[3:-2]  # Extract data bytes from the response
    except serial.SerialException as e:
        logging.error(f"Serial communication error: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error in Modbus communication: {e}")
        return None

# Read multiple holding registers in a single request
def read_holding_registers(ser, register_address, num_registers, config=None):
    if config is None:
        config = get_ac_config()
    try:
        data = send_modbus_request(ser, READ_HOLDING_REGISTERS, register_address, num_registers, config)
        if not data:
            logging.warning(f"Failed to read registers from address {register_address} to {register_address + num_registers - 1}")
            return None
        return struct.unpack(f'>{num_registers}H', data)  # Unpack the 16-bit values
    except Exception as e:
        logging.error(f"Error reading holding registers: {e}")
        return None

# Parse data into meaningful parameters
def parse_pzem_data(registers, config=None):
    if config is None:
        config = get_ac_config()
    try:
        voltage = round(registers[0] * 0.1, PRECISION)
        current = round((registers[2] << 16 | registers[1]) * 0.001, PRECISION)
        power = round((registers[4] << 16 | registers[3]) * 0.1, PRECISION)
        energy = round((registers[6] << 16 | registers[5]), PRECISION)
        frequency = round((registers[7])* 0.1, PRECISION)
        power_factor = round((registers[8])* 0.01, PRECISION)
        if power_factor > 1 or frequency > 60:
            return None
        else:
            return {
                'voltage': voltage,
                'current': current,
                'power': power,
                'energy': energy,
                'frequency': frequency,
                'power_factor': power_factor
            }
    except IndexError as e:
        logging.error(f"Error parsing PZEM data: {e}")
        return None
