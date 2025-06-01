from common.database import (
    db_connection,
    save_hourly_consumption_summary,
    save_hourly_solar_summary,
    save_daily_summary
)
from typing import List, Optional
from .models import HourSummary, HourSummarySolar, DailySummary
from datetime import datetime, timedelta

def get_hourly_consumption_summary() -> List[HourSummary]:
    query = """
        SELECT timestamp, energyConsumption, avgVoltage, avgCurrent, avgPower, avgFrequency, avgPF
        FROM hourSummary
        ORDER BY timestamp DESC
        LIMIT 24
    """
    with db_connection() as connection:
        if not connection:
            return []
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return [HourSummary(**row) for row in rows]

def get_hourly_solar_summary() -> List[HourSummarySolar]:
    query = """
        SELECT timestamp, energyProduced, minVoltage, maxVoltage, avgVoltage, minCurrent, maxCurrent, avgCurrent, minPower, maxPower
        FROM hourSummarySolar
        ORDER BY timestamp DESC
        LIMIT 24
    """
    with db_connection() as connection:
        if not connection:
            return []
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return [HourSummarySolar(**row) for row in rows]

def get_daily_summary() -> List[DailySummary]:
    query = """
        SELECT date, energyConsumption, solarProduction
        FROM dailySummary
        ORDER BY date DESC
        LIMIT 30
    """
    with db_connection() as connection:
        if not connection:
            return []
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return [DailySummary(**row) for row in rows]

def save_hourly_consumption_summary_service(data_batch):
    """
    Save a batch of hourly consumption summary records to the hourSummary table.
    data_batch: List of tuples (timestamp, energyConsumption, avgVoltage, avgCurrent, avgPower, avgFrequency, avgPF)
    """
    with db_connection() as connection:
        if connection:
            save_hourly_consumption_summary(connection, data_batch)


def save_hourly_solar_summary_service(data_batch):
    """
    Save a batch of hourly solar summary records to the hourSummarySolar table.
    data_batch: List of tuples (timestamp, energyProduced, minVoltage, maxVoltage, avgVoltage, minCurrent, maxCurrent, avgCurrent, minPower, maxPower)
    """
    with db_connection() as connection:
        if connection:
            save_hourly_solar_summary(connection, data_batch)


def save_daily_summary_service(data_batch):
    """
    Save a batch of daily summary records to the dailySummary table.
    data_batch: List of tuples (date, energyConsumption, solarProduction)
    """
    with db_connection() as connection:
        if connection:
            save_daily_summary(connection, data_batch)

def update_hourly_consumption_summary():
    """
    Aggregate and save hourly consumption summary from energyConsumption_raw to hourSummary.
    """
    with db_connection() as connection:
        if not connection:
            return
        # Get last processed timestamp
        with connection.cursor() as cursor:
            cursor.execute("SELECT MAX(timestamp) FROM hourSummary")
            result = cursor.fetchone()
            last_timestamp = result['MAX(timestamp)'] if result and result['MAX(timestamp)'] else None
        if last_timestamp is None:
            last_timestamp = '2024-11-01 00:00:00'
        # Aggregate new hourly data
        query = f'''
            SELECT 
                DATE_FORMAT(timestamp, "%%Y-%%m-%%d %%H:00:00") AS hour,
                ROUND(MAX(energy) - MIN(energy), 2) AS energyConsumption,
                ROUND(AVG(voltage), 2) AS avgVoltage,
                ROUND(AVG(current), 2) AS avgCurrent,
                ROUND(AVG(power), 2) AS avgPower,
                ROUND(AVG(frequency), 2) AS avgFrequency,
                ROUND(AVG(power_factor), 2) AS avgPF
            FROM energyConsumption_raw
            WHERE timestamp > %s
            GROUP BY hour
            ORDER BY hour
        '''
        with connection.cursor() as cursor:
            cursor.execute(query, (last_timestamp,))
            rows = cursor.fetchall()
            data_batch = [(
                row['hour'], row['energyConsumption'], row['avgVoltage'], row['avgCurrent'],
                row['avgPower'], row['avgFrequency'], row['avgPF']
            ) for row in rows]
        if data_batch:
            save_hourly_consumption_summary(connection, data_batch)


def update_hourly_solar_summary():
    """
    Aggregate and save hourly solar summary from energyProduction_raw to hourSummarySolar.
    """
    with db_connection() as connection:
        if not connection:
            return
        # Get last processed timestamp
        with connection.cursor() as cursor:
            cursor.execute("SELECT MAX(timestamp) FROM hourSummarySolar")
            result = cursor.fetchone()
            last_timestamp = result['MAX(timestamp)'] if result and result['MAX(timestamp)'] else None
        if last_timestamp is None:
            last_timestamp = '2024-12-03 00:00:00'
        # Aggregate new hourly solar data
        query = '''
            SELECT 
                DATE_FORMAT(timestamp, "%%Y-%%m-%%d %%H:00:00") AS hour,
                ROUND(MAX(energy) - MIN(energy), 2) AS energyProduced,
                ROUND(MIN(voltage), 2) AS minVoltage,
                ROUND(MAX(voltage), 2) AS maxVoltage,
                ROUND(AVG(voltage), 2) AS avgVoltage,
                ROUND(MIN(current), 2) AS minCurrent,
                ROUND(MAX(current), 2) AS maxCurrent,
                ROUND(AVG(current), 2) AS avgCurrent,
                ROUND(MIN(power), 2) AS minPower,
                ROUND(MAX(power), 2) AS maxPower
            FROM energyProduction_raw
            WHERE timestamp > %s
            GROUP BY hour
            ORDER BY hour
        '''
        with connection.cursor() as cursor:
            cursor.execute(query, (last_timestamp,))
            rows = cursor.fetchall()
            data_batch = [(
                row['hour'], row['energyProduced'], row['minVoltage'], row['maxVoltage'], row['avgVoltage'],
                row['minCurrent'], row['maxCurrent'], row['avgCurrent'], row['minPower'], row['maxPower']
            ) for row in rows]
        if data_batch:
            save_hourly_solar_summary(connection, data_batch)


def update_daily_summary():
    """
    Aggregate and save daily summary from energyConsumption_raw and energyProduction_raw to dailySummary.
    """
    with db_connection() as connection:
        if not connection:
            return
        # Get last processed date
        with connection.cursor() as cursor:
            cursor.execute("SELECT MAX(date) FROM dailySummary")
            result = cursor.fetchone()
            last_date = result['MAX(date)'] if result and result['MAX(date)'] else None
        if last_date is None:
            last_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        else:
            last_date = (last_date - timedelta(days=2)).strftime('%Y-%m-%d') if isinstance(last_date, datetime) else last_date
        # Aggregate new daily consumption
        query_consumption = '''
            SELECT 
                DATE(timestamp) AS date,
                ROUND(MAX(energy) - MIN(energy), 2) AS energyConsumption
            FROM energyConsumption_raw
            WHERE DATE(timestamp) > %s
            GROUP BY DATE(timestamp)
        '''
        with connection.cursor() as cursor:
            cursor.execute(query_consumption, (last_date,))
            rows = cursor.fetchall()
            data_batch = [(row['date'], row['energyConsumption'], None) for row in rows]
        if data_batch:
            save_daily_summary(connection, data_batch)
        # Update solar production in daily summary
        query_solar = '''
            SELECT 
                DATE(timestamp) AS date,
                ROUND(MAX(energy) - MIN(energy), 2) AS totalSolarProduction
            FROM energyProduction_raw
            WHERE DATE(timestamp) > %s
            GROUP BY DATE(timestamp)
        '''
        with connection.cursor() as cursor:
            cursor.execute(query_solar, (last_date,))
            rows = cursor.fetchall()
            for row in rows:
                cursor.execute(
                    "UPDATE dailySummary SET solarProduction=%s WHERE date=%s",
                    (row['totalSolarProduction'], row['date'])
                )
            connection.commit()
