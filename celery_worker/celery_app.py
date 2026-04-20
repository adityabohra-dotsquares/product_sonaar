from typing import Dict
import asyncio
import logging
import traceback
from celery import Celery
from dotenv import load_dotenv
from sqlalchemy import update
# from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool

from database import create_sessionmaker
from schemas.import_export import ProductExportRequest
from service.product_import_export import (
    build_export_data,
    build_file_bytes,
)
from utils.gcp_bucket import (
    upload_file_to_gcs,
    download_from_gcs,
)
from service.redis import get_redis_url
from models.b_tasks import BackgroundTask
from apis.v1.utils import _run_import
from database import DATABASE_URL

# --------------------------------------------------
# ENV + LOGGING
# --------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# CELERY APP
# --------------------------------------------------

app = Celery(
    "worker",
    broker=get_redis_url(db=0),
    backend=get_redis_url(db=1),
)

app.conf.update(
    task_track_started=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    worker_prefetch_multiplier=1,
)

# --------------------------------------------------
# EXPORT PRODUCTS TASK
# --------------------------------------------------


@app.task(bind=True, acks_late=True)
def export_products_task(self, payload: Dict):

    async def run():
        engine, SessionLocal = create_sessionmaker()
        try:
            async with SessionLocal() as db:
                await db.execute(
                    update(BackgroundTask)
                    .where(BackgroundTask.task_id == self.request.id)
                    .values(status="RUNNING")
                )
                print("RUNNING")
                await db.commit()

                payload_obj = ProductExportRequest(**payload)

                headers, rows, timestamp = await build_export_data(
                    db,
                    payload_obj.product_ids,
                    filters=payload_obj.filters,
                    sort_key=payload_obj.sort,
                    columns=payload_obj.columns,
                    task_id=self.request.id,
                )
                print("BUILDING")

                file_bytes, filename, content_type = build_file_bytes(
                    headers, rows, timestamp, payload_obj.download_flag
                )
                print("FILE BYTES")

                file_url = upload_file_to_gcs(
                    file_bytes=file_bytes,
                    blob_name=filename,
                    content_type=content_type,
                )
                print("FILE URL")
                await db.execute(
                    update(BackgroundTask)
                    .where(BackgroundTask.task_id == self.request.id)
                    .values(status="COMPLETED", file_url=file_url)
                )
                print("COMPLETED")
                await db.commit()

                return file_url
        finally:
            await engine.dispose()

    try:
        print("STARTING EXPORT-----")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(run())
    except Exception:
        logger.error("Export task failed\n%s", traceback.format_exc())
        raise
    finally:
        loop.close()

# --------------------------------------------------
# IMPORT PRODUCTS TASK
# --------------------------------------------------



@app.task(bind=True, acks_late=True)
def import_products_task(self, file_path: str, job_id: str):
    try:
        logger.info("STARTING IMPORT TASK %s", self.request.id)

        # 1️⃣ Do ALL blocking I/O first (SAFE in prefork)
        local_file = download_from_gcs(file_path)

        # 2️⃣ Run async code in a CLEAN event loop
        def runner():
            return asyncio.run(_run_import(local_file, job_id))

        return runner()

    except Exception:
        logger.error("Import task failed\n%s", traceback.format_exc())
        raise


# --------------------------------------------------
# STOCK CLEANUP TASK
# --------------------------------------------------
from sqlalchemy import update, select

@app.task(bind=True)
def release_expired_reservations_task(self):
    from models.stock_reservation import StockReservation
    from datetime import datetime, timezone

    async def run():
        engine, SessionLocal = create_sessionmaker()
        try:
            async with SessionLocal() as db:
                # Find expired active reservations
                now = datetime.now(timezone.utc)
                stmt = select(StockReservation).where(
                    StockReservation.status == 'active',
                    StockReservation.expires_at < now
                )
                result = await db.execute(stmt)
                expired_reservations = result.scalars().all()
                
                if not expired_reservations:
                    return "No expired reservations found"
                
                count = 0
                for res in expired_reservations:
                    res.status = 'released'
                    res.released_at = now
                    res.released_by = 'system_expired'
                    count += 1
                
                await db.commit()
                return f"Released {count} expired reservations"
        finally:
            await engine.dispose()

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(run())
    except Exception:
        logger.error("Cleanup task failed\n%s", traceback.format_exc())
        raise

# --------------------------------------------------
# BEAT SCHEDULE
# --------------------------------------------------
app.conf.beat_schedule = {
    "release-expired-reservations-every-5-mins": {
        "task": "celery_worker.celery_app.release_expired_reservations_task",
        "schedule": 300.0,  # 5 minutes
    },
}
