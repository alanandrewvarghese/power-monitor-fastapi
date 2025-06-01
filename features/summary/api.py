from fastapi import APIRouter
from .service import (
    get_hourly_consumption_summary,
    get_hourly_solar_summary,
    get_daily_summary,
)
from .models import HourSummary, HourSummarySolar, DailySummary
from .scheduler import start_scheduler

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
