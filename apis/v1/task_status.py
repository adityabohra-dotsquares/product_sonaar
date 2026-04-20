from fastapi import HTTPException
from celery_worker.celery_app import app as celery_app
from fastapi import APIRouter
import os
from fastapi.responses import FileResponse
from celery.result import AsyncResult

router = APIRouter()


@router.get("/export-status/{task_id}")
async def export_status(task_id: str):
    result = AsyncResult(task_id)

    if result.state == "FAILURE" or result.state == "PENDING":
        return {
            "status": "failed",
            "error": result.info,
        }

    if result.state == "SUCCESS":
        return {
            "status": "done",
            "download_url": result.result,
        }

    return {
        "status": result.state.lower(),
        "error": result.info,
    }


@router.get(
    "/download-export/{task_id}",
    responses={
        400: {"description": "Export not ready yet"},
        404: {"description": "File not found"},
    },
)
async def download_export(task_id: str):
    task = celery_app.AsyncResult(task_id)

    if task.state != "SUCCESS":
        raise HTTPException(status_code=400, detail="Export not ready yet")

    file_path = task.result

    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    filename = os.path.basename(file_path)

    return FileResponse(
        file_path, filename=filename, media_type="application/octet-stream"
    )
