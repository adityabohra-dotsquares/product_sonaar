from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime

class ActivityLogOut(BaseModel):
    id: str
    entity_type: str
    entity_id: str
    action: str
    details: Optional[dict[str, Any]] = None
    performed_by: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True

class PaginatedActivityLogs(BaseModel):
    total: int
    page: int
    limit: int
    pages: int
    data: list[ActivityLogOut]
