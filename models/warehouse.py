import uuid
from sqlalchemy import Column, String, Text, ForeignKey, Integer, TIMESTAMP, func
from sqlalchemy.orm import relationship
from database import Base
from sqlalchemy import UniqueConstraint


class Warehouse(Base):
    __tablename__ = "warehouses"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(100), nullable=False)
    location = Column(String(100), nullable=True)
    status = Column(String(100), nullable=False, default="active")

    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    added_by = Column(String(255), nullable=True)
    updated_by = Column(String(255), nullable=True)
    # Relationships
    stocks = relationship(
        "ProductStock", back_populates="warehouse", cascade="all, delete"
    )
    __table_args__ = (
        UniqueConstraint("name", "location", name="uq_warehouse_name_location"),
    )


class ProductStock(Base):
    __tablename__ = "product_stocks"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    product_id = Column(
        String(36), ForeignKey("products.id", ondelete="CASCADE"), nullable=False
    )
    product_identifier = Column(String(255), nullable=True)
    warehouse_id = Column(String(36), ForeignKey("warehouses.id"), nullable=False)
    quantity = Column(Integer, nullable=False, default=0)

    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    added_by = Column(String(255), nullable=True)
    updated_by = Column(String(255), nullable=True)
    product = relationship("Product", back_populates="warehouse_stocks")
    warehouse = relationship("Warehouse", back_populates="stocks")
    variant = relationship("ProductVariant", back_populates="stocks")
    variant_id = Column(
        String(36),
        ForeignKey("product_variants.id", ondelete="CASCADE"),
        nullable=True,
    )
