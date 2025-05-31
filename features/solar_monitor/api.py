from fastapi import APIRouter, BackgroundTasks
from queue import Queue
import threading
from .models import SolarMeasurement, SolarMeasurementBatch
from .service import capture_solar_data, transfer_solar_to_database

router = APIRouter(prefix="/solar", tags=["Solar Monitor"])

data_queue = Queue(maxsize=1000)
stop_event = threading.Event()
threads_started = False

@router.on_event("startup")
def start_solar_background_threads():
    global threads_started
    if not threads_started:
        capture_thread = threading.Thread(target=capture_solar_data, args=(data_queue, stop_event), daemon=True)
        db_thread = threading.Thread(target=transfer_solar_to_database, args=(data_queue, stop_event), daemon=True)
        capture_thread.start()
        db_thread.start()
        threads_started = True

@router.get("/latest", response_model=SolarMeasurement)
def get_latest_solar_measurement():
    try:
        data = data_queue.get_nowait()
        timestamp, voltage, current, power, energy = data
        return SolarMeasurement(
            voltage=voltage,
            current=current,
            power=power,
            energy=energy
        )
    except Exception:
        return {"detail": "No data available"}
