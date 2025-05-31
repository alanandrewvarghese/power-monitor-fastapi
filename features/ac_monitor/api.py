from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import StreamingResponse
from queue import Queue
import threading
import asyncio
import json
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

async def ac_event_generator():
    last_sent = None
    while True:
        try:
            if not data_queue.empty():
                data = data_queue.get_nowait()
                timestamp, voltage, current, power, energy, frequency, power_factor = data
                measurement = ACMeasurement(
                    voltage=voltage,
                    current=current,
                    power=power,
                    energy=energy,
                    frequency=frequency,
                    power_factor=power_factor
                )
                current_data = measurement.dict()
                
                # Only send if data has changed
                if current_data != last_sent:
                    yield f"data: {json.dumps(current_data)}\n\n"
                    last_sent = current_data
            await asyncio.sleep(0.1)  # Adjust the interval as needed (100ms here)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Error in AC event generator: {e}")
            await asyncio.sleep(1)

@router.get("/latest/live")
async def live_ac_measurements(request: Request):
    return StreamingResponse(
        ac_event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # Disable buffering for nginx
        }
    )

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