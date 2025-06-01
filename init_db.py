import pymysql
from config import get_database_config

CONFIG = get_database_config()

def create_database_if_not_exists():
    connection = pymysql.connect(
        host=CONFIG['db_host'],
        user=CONFIG['db_user'],
        password=CONFIG['db_password'],
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{CONFIG['db_name']}`")
        connection.commit()
        print(f"Database '{CONFIG['db_name']}' checked/created.")
    finally:
        connection.close()

def create_tables():
    connection = pymysql.connect(
        host=CONFIG['db_host'],
        user=CONFIG['db_user'],
        password=CONFIG['db_password'],
        database=CONFIG['db_name'],
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS energyProduction_raw (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME NOT NULL,
                    voltage FLOAT NOT NULL,
                    current FLOAT NOT NULL,
                    power FLOAT NOT NULL,
                    energy FLOAT NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS energyConsumption_raw (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME NOT NULL,
                    voltage FLOAT NOT NULL,
                    current FLOAT NOT NULL,
                    power FLOAT NOT NULL,
                    energy FLOAT NOT NULL,
                    frequency FLOAT NOT NULL,
                    power_factor FLOAT NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hourSummary (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME NOT NULL,
                    energyConsumption FLOAT NOT NULL,
                    avgVoltage FLOAT NOT NULL,
                    avgCurrent FLOAT NOT NULL,
                    avgPower FLOAT NOT NULL,
                    avgFrequency FLOAT NOT NULL,
                    avgPF FLOAT NOT NULL,
                    UNIQUE(timestamp)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hourSummarySolar (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME NOT NULL,
                    energyProduced FLOAT NOT NULL,
                    minVoltage FLOAT NOT NULL,
                    maxVoltage FLOAT NOT NULL,
                    avgVoltage FLOAT NOT NULL,
                    minCurrent FLOAT NOT NULL,
                    maxCurrent FLOAT NOT NULL,
                    avgCurrent FLOAT NOT NULL,
                    minPower FLOAT NOT NULL,
                    maxPower FLOAT NOT NULL,
                    UNIQUE(timestamp)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dailySummary (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    date DATE NOT NULL,
                    energyConsumption FLOAT,
                    solarProduction FLOAT,
                    UNIQUE(date)
                )
            """)
        connection.commit()
        print("Tables created successfully.")
    finally:
        connection.close()

if __name__ == "__main__":
    create_database_if_not_exists()
    create_tables()