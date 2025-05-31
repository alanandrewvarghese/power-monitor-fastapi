import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# AC system configuration
def get_ac_config():
    return {
        'serial_port': os.getenv('AC_SERIAL_PORT', '/dev/ttyUSB0'),
        'baud_rate': int(os.getenv('AC_BAUD_RATE', 9600)),
        'serial_timeout': int(os.getenv('AC_SERIAL_TIMEOUT', 1)),
        'db_host': os.getenv('AC_DB_HOST', 'localhost'),
        'db_user': os.getenv('AC_DB_USER', 'python'),
        'db_password': os.getenv('AC_DB_PASSWORD', 'pymysql'),
        'db_name': os.getenv('AC_DB_NAME', 'PowerMon'),
        'slave_address': int(os.getenv('AC_SLAVE_ADDRESS', '1'), 16)
    }

# Solar system configuration
def get_solar_config():
    return {
        'serial_port': os.getenv('SOLAR_SERIAL_PORT', '/dev/ttyUSB1'),
        'baud_rate': int(os.getenv('SOLAR_BAUD_RATE', 9600)),
        'serial_timeout': int(os.getenv('SOLAR_SERIAL_TIMEOUT', 1)),
        'db_host': os.getenv('SOLAR_DB_HOST', 'localhost'),
        'db_user': os.getenv('SOLAR_DB_USER', 'python'),
        'db_password': os.getenv('SOLAR_DB_PASSWORD', 'pymysql'),
        'db_name': os.getenv('SOLAR_DB_NAME', 'PowerMon'),
        'slave_address': int(os.getenv('SOLAR_SLAVE_ADDRESS', '2'), 16)
    }
    
def get_database_config():
    return {
        'db_host': os.getenv('DB_HOST', 'localhost'),
        'db_user': os.getenv('DB_USER', 'python'),
        'db_password': os.getenv('DB_PASSWORD', 'pymysql'),
        'db_name': os.getenv('DB_NAME', 'PowerMon')
    }

# Precision for both systems
PRECISION = 4
