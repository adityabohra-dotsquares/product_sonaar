from typing import Annotated
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, delete

from models.b_tasks import BackgroundTask
from deps import get_db

router = APIRouter()   

@router.get("/list-background-tasks")
async def list_background_tasks(
    db: Annotated[AsyncSession, Depends(get_db)],
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
):
    offset = (page - 1) * limit

    # Total count
    total_stmt = select(func.count(BackgroundTask.id))
    total = await db.scalar(total_stmt)

    # Fetch data
    stmt = (
        select(
            BackgroundTask.id,
            BackgroundTask.task_id,
            BackgroundTask.task_type,
            BackgroundTask.task_info,
            BackgroundTask.status,
            BackgroundTask.file_url,
            BackgroundTask.created_at,
            BackgroundTask.updated_at,
            BackgroundTask.added_by,
            BackgroundTask.updated_by
        )
        .order_by(BackgroundTask.created_at.desc())
        .offset(offset)
        .limit(limit)
    )

    result = await db.execute(stmt)
    tasks = result.mappings().all()

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "pages": (total + limit - 1) // limit,
        "data": tasks,
    }

@router.get(
    "/details-background-tasks/{task_id}",
    responses={
        404: {"description": "Task not found."},
    },
)
async def get_background_task_details(
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    stmt = select(BackgroundTask).where(BackgroundTask.id == task_id)
    result = await db.execute(stmt)

    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return task


@router.delete(
    "/delete-background-task/{task_id}",
    responses={
        404: {"description": "Task not found."},
    },
)
async def delete_background_task(
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    stmt = select(BackgroundTask).where(BackgroundTask.id == task_id)
    result = await db.execute(stmt)

    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    await db.delete(task)
    await db.commit()

    return {"message": "Background task deleted successfully"}



@router.delete(
    "/delete-background-task/status/{status}",
    responses={
        404: {"description": "No tasks found with specified status."},
    },
)
async def delete_background_tasks_by_status(
    status: str,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    stmt = delete(BackgroundTask).where(BackgroundTask.status == status)
    result = await db.execute(stmt)
    await db.commit()

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"No tasks found with status: {status}")

    return {"message": f"Successfully deleted {result.rowcount} background tasks with status: {status}"}


