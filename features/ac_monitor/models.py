from pydantic import BaseModel

class ACMeasurement(BaseModel):
    voltage: float
    current: float
    power: float
    energy: float
    frequency: float
    power_factor: float

class ACMeasurementBatch(BaseModel):
    measurements: list[ACMeasurement]
    timestamp: str  # ISO format or datetime, adjust as needed
