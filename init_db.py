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
        connection.commit()
        print("Tables created successfully.")
    finally:
        connection.close()

if __name__ == "__main__":
    create_database_if_not_exists()
    create_tables()