from fastapi import FastAPI
from contextlib import asynccontextmanager
from routes import router
from db import setup_database
from logging_config import setup_logging, get_logger

# --- FastAPI Application Initialization ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Configure logging (idempotent)
    setup_logging()
    logger = get_logger("sms_sync.app")
    # Startup: ensure DB table exists
    setup_database()
    yield
    # Shutdown: add cleanup here if needed

app = FastAPI(
    title="SMS Sync Service",
    description="API to receive and store SMS data from an Android app.",
    version="1.0.0",
    lifespan=lifespan,
)

# Mount API routes
app.include_router(router)

# Startup handled via FastAPI lifespan

"""
Routes are defined in `routes.py` and mounted via `app.include_router(router)`.
"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

