import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.config import Settings
from app.observability.metrics_collector import MetricsCollector
from app.observability.metrics_repository import MetricsRepository


logging.basicConfig(level=getattr(logging, Settings.LOG_LEVEL.strip(), logging.INFO))
logger = logging.getLogger(__name__)

repository = MetricsRepository()
collector = MetricsCollector(repository=repository)


@asynccontextmanager
async def lifespan(app: FastAPI):
    collector.start()
    try:
        yield
    finally:
        collector.stop()


app = FastAPI(
    title="RFB Loader Enterprise Metrics API",
    version=Settings.APP_VERSION,
    lifespan=lifespan,
)


@app.get("/health")
def health():
    return {
        "status": "OK",
        "app": Settings.APP_NAME,
        "version": Settings.APP_VERSION,
        "metrics_collection_interval": Settings.METRICS_COLLECTION_INTERVAL,
    }


@app.get("/metrics")
def prometheus_metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/dashboard/overview")
def dashboard_overview():
    return repository.get_overview_metrics()


@app.get("/dashboard/performance")
def dashboard_performance():
    return repository.get_performance_metrics()


@app.get("/dashboard/database")
def dashboard_database():
    return repository.get_database_counts()


@app.get("/dashboard/execution")
def dashboard_execution():
    return repository.get_execution_history()


@app.get("/dashboard/promotion")
def dashboard_promotion():
    return repository.get_promotion_metrics()


@app.get("/dashboard/audit")
def dashboard_audit():
    return repository.get_audit_metrics()
