from fastapi import APIRouter, Query
from .service import (
    get_hourly_consumption_summary,
    get_hourly_solar_summary,
    get_daily_summary,
)
from .models import HourSummary, HourSummarySolar, DailySummary
from .scheduler import start_scheduler
from typing import Optional
from datetime import datetime
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/summary", tags=["Summary"])

@router.on_event("startup")
def on_startup():
    start_scheduler()

@router.get("/hourly/consumption", response_model=list[HourSummary])
def hourly_consumption_summary():
    return get_hourly_consumption_summary()

@router.get("/hourly/solar", response_model=list[HourSummarySolar])
def hourly_solar_summary():
    return get_hourly_solar_summary()

@router.get("/daily", response_model=list[DailySummary])
def daily_summary():
    return get_daily_summary()

@router.get("/energy-at-midnight")
def get_energy_at_midnight(date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format. Defaults to today.")):
    """
    Get the energy values from energyConsumption_raw and energyProduction_raw at 00:00 for a given date (default: today).
    """
    from common.database import db_connection
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    with db_connection() as connection:
        if not connection:
            return JSONResponse(status_code=500, content={"detail": "Database connection error"})
        with connection.cursor() as cursor:
            # Get the first record at or after 00:00 for the date
            cursor.execute("""
                SELECT energy FROM energyConsumption_raw
                WHERE timestamp >= %s AND timestamp < DATE_ADD(%s, INTERVAL 1 DAY)
                ORDER BY timestamp ASC LIMIT 1
            """, (f"{date} 00:00:00", date))
            row_c = cursor.fetchone()
            cursor.execute("""
                SELECT energy FROM energyProduction_raw
                WHERE timestamp >= %s AND timestamp < DATE_ADD(%s, INTERVAL 1 DAY)
                ORDER BY timestamp ASC LIMIT 1
            """, (f"{date} 00:00:00", date))
            row_p = cursor.fetchone()
    return {
        "date": date,
        "energyConsumptionAtMidnight": row_c["energy"] if row_c else None,
        "energyProductionAtMidnight": row_p["energy"] if row_p else None
    }
