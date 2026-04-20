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
    DateTime,
    Index,
)
from sqlalchemy.orm import relationship
import uuid
from database import Base


class ReturnPolicy(Base):
    __tablename__ = "return_policies"

    id = Column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        unique=True,
        index=True,
    )
    scope_type = Column(
        String(20), index=True
    )  # 'product' | 'category' | 'brand' | 'global'
    scope_id = Column(String(36), index=True, nullable=True)
    country_code = Column(String(2), index=True, nullable=True)  # country code
    days = Column(Integer, nullable=False)
    restocking_fee_pct = Column(Integer, default=0)
    text = Column(Text, nullable=False)
    status = Column(String(20), default="active")
    priority = Column(Integer, default=0)
    starts_at = Column(DateTime, nullable=True)
    ends_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_policy_scope_combo", "scope_type", "scope_id", "country_code", "priority"),
    )
