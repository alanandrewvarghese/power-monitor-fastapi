# Solar Pydantic models

from pydantic import BaseModel

class SolarMeasurement(BaseModel):
    voltage: float
    current: float
    power: float
    energy: float

class SolarMeasurementBatch(BaseModel):
    measurements: list[SolarMeasurement]
    timestamp: str  # ISO format or datetime, adjust as needed
