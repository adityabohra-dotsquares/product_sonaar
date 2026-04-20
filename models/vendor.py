from sqlalchemy import (
    Column,
    String,
    Integer,
    Text,
    DECIMAL,
    ForeignKey,
    TIMESTAMP,
    func,
    Boolean,
    Float,
    UniqueConstraint,
    JSON,
)
from sqlalchemy.orm import relationship
import uuid
from database import Base


class Vendor(Base):
    __tablename__ = "vendors"

    id = Column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        unique=True,
        index=True,
    )
    name = Column(String(255), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    added_by = Column(String(255), nullable=True)
    updated_by = Column(String(255), nullable=True)

