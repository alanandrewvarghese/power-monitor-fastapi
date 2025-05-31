# Solar Modbus communication

import struct
import serial
import logging
from config import get_solar_config, PRECISION

READ_HOLDING_REGISTERS = 0x04

CRC16_TABLE = [0x0000, 0xA001] + [0] * 254
for i in range(1, 256):
    crc = i
    for _ in range(8):
        if crc & 0x0001:
            crc = (crc >> 1) ^ 0xA001
        else:
            crc >>= 1
    CRC16_TABLE[i] = crc

def calculate_crc(data):
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        crc = (crc >> 8) ^ CRC16_TABLE[crc & 0xFF]
    return struct.pack('<H', crc)

def send_modbus_request(ser, function_code, register_address, num_registers, config=None):
    if config is None:
        config = get_solar_config()
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
        return response[3:-2]
    except serial.SerialException as e:
        logging.error(f"Serial communication error: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error in Modbus communication: {e}")
        return None

def read_holding_registers(ser, register_address, num_registers, config=None):
    if config is None:
        config = get_solar_config()
    try:
        data = send_modbus_request(ser, READ_HOLDING_REGISTERS, register_address, num_registers, config)
        if not data:
            logging.warning(f"Failed to read registers from address {register_address} to {register_address + num_registers - 1}")
            return None
        return struct.unpack(f'>{num_registers}H', data)
    except Exception as e:
        logging.error(f"Error reading holding registers: {e}")
        return None

def parse_pzem_data(registers, config=None):
    if config is None:
        config = get_solar_config()
    try:
        voltage = round(registers[0] * 0.01, PRECISION)
        current = round((registers[1]) * 0.01, PRECISION)
        power = round((registers[3] << 16 | registers[2]) * 0.1, PRECISION)
        energy = round((registers[5] << 16 | registers[4]), PRECISION)
        if power == 0 or current == 0:
            return None
        else:
            return {
                'voltage': voltage,
                'current': current,
                'power': power,
                'energy': energy
            }
    except IndexError as e:
        logging.error(f"Error parsing PZEM data: {e}")
        return None
