import pymysql
import logging
from contextlib import contextmanager
from config import get_database_config

CONFIG = get_database_config()

@contextmanager
def db_connection():
    connection = None
    try:
        connection = pymysql.connect(
            host=CONFIG['db_host'],
            user=CONFIG['db_user'],
            password=CONFIG['db_password'],
            database=CONFIG['db_name'],
            cursorclass=pymysql.cursors.DictCursor
        )
        yield connection
    except pymysql.MySQLError as e:
        logging.error(f"Database connection error: {e}")
        yield None
    finally:
        if connection:
            connection.close()

def log_to_db_production(connection, data_batch):
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO energyProduction_raw (timestamp, voltage, current, power, energy)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.executemany(sql, data_batch)
        connection.commit()
        logging.info("Batch data logged successfully (production)")
    except pymysql.MySQLError as e:
        logging.error(f"Error inserting data into MySQL (production): {e}")

def log_to_db_consumption(connection, data_batch):
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO energyConsumption_raw (timestamp, voltage, current, power, energy, frequency, power_factor)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(sql, data_batch)
        connection.commit()
        logging.info("Batch data logged successfully (consumption)")
    except pymysql.MySQLError as e:
        logging.error(f"Error inserting data into MySQL (consumption): {e}")

def save_hourly_consumption_summary(connection, data_batch):
    """
    Save a batch of hourly consumption summary records to the hourSummary table.
    Each item in data_batch should be a tuple:
    (timestamp, energyConsumption, avgVoltage, avgCurrent, avgPower, avgFrequency, avgPF)
    """
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO hourSummary (timestamp, energyConsumption, avgVoltage, avgCurrent, avgPower, avgFrequency, avgPF)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                energyConsumption=VALUES(energyConsumption),
                avgVoltage=VALUES(avgVoltage),
                avgCurrent=VALUES(avgCurrent),
                avgPower=VALUES(avgPower),
                avgFrequency=VALUES(avgFrequency),
                avgPF=VALUES(avgPF)
            """
            cursor.executemany(sql, data_batch)
        connection.commit()
        logging.info("Batch hourly consumption summary saved successfully.")
    except pymysql.MySQLError as e:
        logging.error(f"Error inserting hourly consumption summary: {e}")

def save_hourly_solar_summary(connection, data_batch):
    """
    Save a batch of hourly solar summary records to the hourSummarySolar table.
    Each item in data_batch should be a tuple:
    (timestamp, energyProduced, minVoltage, maxVoltage, avgVoltage, minCurrent, maxCurrent, avgCurrent, minPower, maxPower)
    """
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO hourSummarySolar (timestamp, energyProduced, minVoltage, maxVoltage, avgVoltage, minCurrent, maxCurrent, avgCurrent, minPower, maxPower)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                energyProduced=VALUES(energyProduced),
                minVoltage=VALUES(minVoltage),
                maxVoltage=VALUES(maxVoltage),
                avgVoltage=VALUES(avgVoltage),
                minCurrent=VALUES(minCurrent),
                maxCurrent=VALUES(maxCurrent),
                avgCurrent=VALUES(avgCurrent),
                minPower=VALUES(minPower),
                maxPower=VALUES(maxPower)
            """
            cursor.executemany(sql, data_batch)
        connection.commit()
        logging.info("Batch hourly solar summary saved successfully.")
    except pymysql.MySQLError as e:
        logging.error(f"Error inserting hourly solar summary: {e}")

def save_daily_summary(connection, data_batch):
    """
    Save a batch of daily summary records to the dailySummary table.
    Each item in data_batch should be a tuple:
    (date, energyConsumption, solarProduction)
    """
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO dailySummary (date, energyConsumption, solarProduction)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                energyConsumption=VALUES(energyConsumption),
                solarProduction=VALUES(solarProduction)
            """
            cursor.executemany(sql, data_batch)
        connection.commit()
        logging.info("Batch daily summary saved successfully.")
    except pymysql.MySQLError as e:
        logging.error(f"Error inserting daily summary: {e}")
