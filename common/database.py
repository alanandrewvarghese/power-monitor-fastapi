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
