"""
GovContract-Alpha - FastAPI Application
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import get_settings
from database import init_db
from api import signals_router, companies_router

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Startup
    print("🚀 Starting GovContract-Alpha...")
    await init_db()
    print("✅ Database initialized")
    
    # Start scheduler for auto-fetching
    from pipeline.scheduler import start_scheduler
    start_scheduler()
    print(f"⏰ Pipeline auto-fetching every {settings.pipeline_interval_minutes} minutes")
    
    yield
    
    # Shutdown
    print("👋 Shutting down GovContract-Alpha...")


app = FastAPI(
    title="GovContract-Alpha",
    description="Government Contract Signal Detection API",
    version="1.0.0",
    lifespan=lifespan
)

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for Railway deployment
    allow_credentials=False,  # Must be False when using wildcard origins
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
app.include_router(signals_router, prefix="/api")
app.include_router(companies_router, prefix="/api")


@app.get("/")
async def root():
    return {
        "name": "GovContract-Alpha",
        "status": "operational",
        "docs": "/docs"
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


# Pipeline trigger endpoint (for manual runs)
@app.post("/api/pipeline/run")
async def run_pipeline():
    """Manually trigger the data pipeline"""
    from pipeline.scheduler import run_pipeline_now
    result = await run_pipeline_now()
    return result
