from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime

class HourSummary(BaseModel):
    timestamp: datetime
    energyConsumption: float
    avgVoltage: float
    avgCurrent: float
    avgPower: float
    avgFrequency: float
    avgPF: float

class HourSummarySolar(BaseModel):
    timestamp: datetime
    energyProduced: float
    minVoltage: float
    maxVoltage: float
    avgVoltage: float
    minCurrent: float
    maxCurrent: float
    avgCurrent: float
    minPower: float
    maxPower: float

class DailySummary(BaseModel):
    date: date
    energyConsumption: Optional[float] = None
    solarProduction: Optional[float] = None
