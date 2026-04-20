from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc
from typing import Optional, Annotated
from datetime import datetime
from deps import get_db
from models.activity_log import ActivityLog
from schemas.activity_log import PaginatedActivityLogs

router = APIRouter()

@router.get("/list", response_model=PaginatedActivityLogs)
async def list_activity_logs(
    db: Annotated[AsyncSession, Depends(get_db)],
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    entity_type: Annotated[
        Optional[str],
        Query(description="Filter by entity type (e.g. product, brand, category)"),
    ] = None,
    action: Annotated[
        Optional[str], Query(description="Filter by action (create, update, delete)")
    ] = None,
    entity_id: Annotated[Optional[str], Query(description="Filter by specific entity ID")] = None,
    performed_by: Annotated[
        Optional[str], Query(description="Filter by user who performed action")
    ] = None,
    start_date: Annotated[
        Optional[datetime], Query(description="Filter logs starting from this date")
    ] = None,
    end_date: Annotated[
        Optional[datetime], Query(description="Filter logs up to this date")
    ] = None,
):
    offset = (page - 1) * limit
    
    # Base query
    query = select(ActivityLog)
    
    # Filters
    filters = []
    if entity_type:
        filters.append(ActivityLog.entity_type == entity_type)
    if action:
        filters.append(ActivityLog.action == action)
    if entity_id:
        filters.append(ActivityLog.entity_id == entity_id)
    if performed_by:
        filters.append(ActivityLog.performed_by == performed_by)
    if start_date:
        filters.append(ActivityLog.created_at >= start_date)
    if end_date:
        filters.append(ActivityLog.created_at <= end_date)
        
    if filters:
        query = query.where(*filters)
        
    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar_one()
    
    # Execute query with pagination and ordering
    query = query.order_by(desc(ActivityLog.created_at)).offset(offset).limit(limit)
    result = await db.execute(query)
    logs = result.scalars().all()
    
    return {
        "total": total,
        "page": page,
        "limit": limit,
        "pages": (total + limit - 1) // limit,
        "data": logs
    }
