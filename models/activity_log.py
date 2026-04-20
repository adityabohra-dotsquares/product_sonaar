from sqlalchemy import Column, String, TIMESTAMP, func, JSON
import uuid
from database import Base

class ActivityLog(Base):
    __tablename__ = "activity_logs"

    id = Column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        unique=True,
        index=True,
    )
    entity_type = Column(String(50), nullable=False) # e.g., "product"
    entity_id = Column(String(36), nullable=False)
    action = Column(String(50), nullable=False) # "create", "update", "delete"
    details = Column(JSON, nullable=True)
    performed_by = Column(String(255), nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
