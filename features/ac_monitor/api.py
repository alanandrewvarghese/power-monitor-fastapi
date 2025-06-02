from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import StreamingResponse
from queue import Queue
import threading
import asyncio
import json
from .models import ACMeasurement, ACMeasurementBatch
from .service import capture_ac_data, transfer_ac_to_database

router = APIRouter(prefix="/ac", tags=["AC Monitor"])

source_ac_data_queue = Queue(maxsize=1000)  # Renamed from data_queue
ac_client_sse_queues = []  # New global list for client SSE queues
stop_event = threading.Event()
threads_started = False

@router.on_event("startup")
def start_ac_background_threads():
    global threads_started
    if not threads_started:
        capture_thread = threading.Thread(target=capture_ac_data, args=(source_ac_data_queue, stop_event), daemon=True)
        db_thread = threading.Thread(target=transfer_ac_to_database, args=(source_ac_data_queue, stop_event), daemon=True)
        capture_thread.start()
        db_thread.start()
        # Start the new ac_data_forwarder task
        asyncio.create_task(ac_data_forwarder())
        threads_started = True

async def ac_data_forwarder():
    """
    Continuously gets data from source_ac_data_queue and puts it into all client_sse_queues.
    """
    while True:
        try:
            data_item = source_ac_data_queue.get_nowait()  # Non-blocking get
            # Iterate over a copy of the list to avoid issues if it's modified during iteration
            for client_q in list(ac_client_sse_queues):
                try:
                    await client_q.put(data_item)
                except asyncio.QueueFull:
                    # Handle if a specific client queue is full (optional, default asyncio.Queue is unbounded)
                    print(f"Client queue {client_q} is full. Data item for this client lost.")
                except Exception as e:
                    # Other exceptions related to putting data into a client's queue
                    print(f"Error putting data to client queue {client_q}: {e}")
        except Exception as e: # Should be queue.Empty, but catching broader for safety
            # Queue is empty, wait a bit before trying again to avoid busy-waiting
            await asyncio.sleep(0.05)


async def ac_event_generator(request: Request): # Added request parameter
    client_queue = asyncio.Queue(maxsize=100) # Each client gets its own asyncio Queue
    ac_client_sse_queues.append(client_queue)
    last_sent = None
    try:
        while True:
            try:
                # Wait for an item from the client-specific queue
                item = await client_queue.get()
                timestamp, voltage, current, power, energy, frequency, power_factor = item
                measurement = ACMeasurement(
                    voltage=voltage,
                    current=current,
                    power=power,
                    energy=energy,
                    frequency=frequency,
                    power_factor=power_factor
                )
                current_data = measurement.dict()

                if current_data != last_sent:
                    yield f"data: {json.dumps(current_data)}\n\n"
                    last_sent = current_data
                client_queue.task_done() # Notify the queue that the item has been processed
            except asyncio.CancelledError:
                # This exception is raised when the client disconnects.
                break
            except Exception as e:
                print(f"Error in AC event generator for a client: {e}")
                # Depending on the error, you might want to break or continue
                await asyncio.sleep(1) # Avoid tight loop on persistent error
    finally:
        # Ensure the client's queue is removed from the global list
        if client_queue in ac_client_sse_queues:
            ac_client_sse_queues.remove(client_queue)
        print(f"Client {request.client} disconnected, queue removed. Remaining queues: {len(ac_client_sse_queues)}")


@router.get("/latest/live")
async def live_ac_measurements(request: Request):
    return StreamingResponse(
        ac_event_generator(request), # Pass the request to the generator
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
        # Get data from the source_ac_data_queue
        data = source_ac_data_queue.get_nowait()
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