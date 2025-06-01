from fastapi import FastAPI
from features.ac_monitor.api import router as ac_router
from features.solar_monitor.api import router as solar_router
from features.summary.api import router as summary_router
from common.logging import setup_logging
from fastapi.middleware.cors import CORSMiddleware


# Entry point for FastAPI app

setup_logging()
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ac_router)
app.include_router(solar_router)
app.include_router(summary_router)
