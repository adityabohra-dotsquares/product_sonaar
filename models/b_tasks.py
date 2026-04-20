# models.py
from sqlalchemy import (
    Column,
    Integer,
    String,
    TIMESTAMP,
    JSON,
    func,
    Index,
    text,
    UniqueConstraint,
)
from database import Base
import uuid


class BackgroundTask(Base):
    __tablename__ = "background_tasks"

    id = Column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        index=True,
    )
    task_id = Column(String(100), nullable=True)
    task_type = Column(String(100), nullable=True)
    status = Column(String(100), nullable=True)
    file_url=Column(String(255), nullable=True)
    task_info=Column(JSON, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(),index=True)
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    added_by = Column(String(255), nullable=True)
    updated_by = Column(String(255), nullable=True)

  
