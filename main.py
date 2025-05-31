from fastapi import FastAPI
from features.ac_monitor.api import router as ac_router
from features.solar_monitor.api import router as solar_router

# Entry point for FastAPI app

app = FastAPI()

app.include_router(ac_router)
app.include_router(solar_router)
