import threading
import time
from datetime import datetime
from .service import (
    update_hourly_consumption_summary,
    update_hourly_solar_summary,
    update_daily_summary
)

def scheduler_thread():
    """
    Scheduler thread to run hourly and daily summary aggregation automatically.
    """
    last_hour = None
    last_day = None
    while True:
        now = datetime.now()
        # Run hourly tasks exactly at the start of each hour
        if now.minute == 0 and now.second < 10 and last_hour != now.hour:
            update_hourly_consumption_summary()
            update_hourly_solar_summary()
            last_hour = now.hour
        # Run daily task at midnight
        if now.hour == 0 and now.minute == 0 and now.second < 10 and last_day != now.date():
            update_daily_summary()
            last_day = now.date()
        time.sleep(5)  # Check every 5 seconds for better accuracy

def start_scheduler():
    t = threading.Thread(target=scheduler_thread, daemon=True)
    t.start()
