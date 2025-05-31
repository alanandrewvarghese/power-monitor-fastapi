from fastapi import APIRouter, BackgroundTasks
from queue import Queue
import threading
from .models import ACMeasurement, ACMeasurementBatch
from .service import capture_ac_data, transfer_ac_to_database

router = APIRouter(prefix="/ac", tags=["AC Monitor"])

data_queue = Queue(maxsize=1000)
stop_event = threading.Event()
threads_started = False

@router.on_event("startup")
def start_ac_background_threads():
    global threads_started
    if not threads_started:
        capture_thread = threading.Thread(target=capture_ac_data, args=(data_queue, stop_event), daemon=True)
        db_thread = threading.Thread(target=transfer_ac_to_database, args=(data_queue, stop_event), daemon=True)
        capture_thread.start()
        db_thread.start()
        threads_started = True

@router.get("/latest", response_model=ACMeasurement)
def get_latest_ac_measurement():
    try:
        data = data_queue.get_nowait()
        timestamp, voltage, current, power, energy, frequency, power_factor = data
        return ACMeasurement(
            voltage=voltage,
            current=current,
            power=power,
            energy=energy,
            frequency=frequency,
            power_factor=power_factor
        )
    except Exception:
        return {"detail": "No data available"}
