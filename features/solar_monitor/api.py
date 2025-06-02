from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import StreamingResponse
from queue import Queue
import threading
import asyncio
import json
from .models import SolarMeasurement, SolarMeasurementBatch
from .service import capture_solar_data, transfer_solar_to_database

router = APIRouter(prefix="/solar", tags=["Solar Monitor"])

source_solar_data_queue = Queue(maxsize=1000)  # Renamed from data_queue
solar_client_sse_queues = []  # New global list for client SSE queues
stop_event = threading.Event()
threads_started = False

@router.on_event("startup")
def start_solar_background_threads():
    global threads_started
    if not threads_started:
        capture_thread = threading.Thread(target=capture_solar_data, args=(source_solar_data_queue, stop_event), daemon=True)
        db_thread = threading.Thread(target=transfer_solar_to_database, args=(source_solar_data_queue, stop_event), daemon=True)
        capture_thread.start()
        db_thread.start()
        # Start the new solar_data_forwarder task
        asyncio.create_task(solar_data_forwarder())
        threads_started = True

async def solar_data_forwarder():
    """
    Continuously gets data from source_solar_data_queue and puts it into all client_sse_queues.
    """
    while True:
        try:
            data_item = source_solar_data_queue.get_nowait()  # Non-blocking get
            # Iterate over a copy of the list to avoid issues if it's modified during iteration
            for client_q in list(solar_client_sse_queues):
                try:
                    await client_q.put(data_item)
                except asyncio.QueueFull:
                    print(f"Solar client queue {client_q} is full. Data item for this client lost.")
                except Exception as e:
                    print(f"Error putting data to solar client queue {client_q}: {e}")
        except Exception as e: # Should be queue.Empty, but catching broader for safety
            await asyncio.sleep(0.05) # Queue is empty, wait a bit

async def event_generator(request: Request): # Renamed, now specific to solar & takes request
    client_queue = asyncio.Queue(maxsize=100)
    solar_client_sse_queues.append(client_queue)
    last_sent = None
    try:
        while True:
            try:
                item = await client_queue.get()
                timestamp, voltage, current, power, energy = item
                measurement = SolarMeasurement(
                    voltage=voltage,
                    current=current,
                    power=power,
                    energy=energy
                )
                current_data = measurement.dict()
                
                if current_data != last_sent:
                    yield f"data: {json.dumps(current_data)}\n\n"
                    last_sent = current_data
                client_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in solar event generator for a client: {e}")
                await asyncio.sleep(1)
    finally:
        if client_queue in solar_client_sse_queues:
            solar_client_sse_queues.remove(client_queue)
        print(f"Solar client {request.client} disconnected, queue removed. Remaining queues: {len(solar_client_sse_queues)}")

@router.get("/latest/live")
async def live_solar_measurements(request: Request):
    return StreamingResponse(
        event_generator(request), # Pass request to the generator
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )

@router.get("/latest", response_model=SolarMeasurement)
def get_latest_solar_measurement():
    try:
        data = source_solar_data_queue.get_nowait() # Use the new queue name
        timestamp, voltage, current, power, energy = data
        return SolarMeasurement(
            voltage=voltage,
            current=current,
            power=power,
            energy=energy
        )
    except Exception:
        return {"detail": "No data available"}