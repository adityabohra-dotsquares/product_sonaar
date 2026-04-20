# models/promotion.py

from sqlalchemy import (
    Column,
    Integer,
    Float,
    String,
    ForeignKey,
    DateTime,
    func,
    Enum,
    DECIMAL,
)
from database import Base
from datetime import datetime
import uuid
from sqlalchemy import UniqueConstraint


class Promotion(Base):
    __tablename__ = "promotions"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    offer_name = Column(String(255), nullable=False)
    offer_type = Column(
        String(50), nullable=False
    )  # offer on product or category or brand
    reference_id = Column(
        String(36), nullable=True
    )  # product_id or category_id or brand_id
    discount_type = Column(String(50), nullable=False)  # percentage or fixed
    discount_percentage = Column(Float, nullable=True)
    discount_value = Column(Float, nullable=False)
    max_discount_amount = Column(DECIMAL(12, 2), nullable=True)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    description = Column(String(500), nullable=True)
    status = Column(String(50), default="active", nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    added_by = Column(String(36), nullable=True)
    updated_by = Column(String(36), nullable=True)
    original_price = Column(DECIMAL(12, 2), nullable=True)
    discounted_price = Column(DECIMAL(12, 2), nullable=True)

    @property
    def is_expired(self) -> bool:
        return datetime.now() > self.end_date

    @property
    def current_status(self) -> str:
        if self.is_expired:
            return "inactive"
        return self.status

    # __table_args__ = (
    #     UniqueConstraint(
    #         "offer_type", "reference_id", "status", name="uq_active_promo"
    #     ),
    # )
