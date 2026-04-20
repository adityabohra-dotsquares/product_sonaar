from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base  # your declarative base
import uuid


class Brand(Base):
    __tablename__ = "brands"

    id = Column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),  
        index=True,
    )
    name = Column(String(100), unique=True, nullable=False)
    slug = Column(String(100), unique=True, nullable=True, index=True)
    logo_url = Column(String(255), nullable=True)
    image_url = Column(String(255), nullable=True) 
    is_active = Column(Boolean(), default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    added_by=Column(String(255),nullable=True)
    updated_by=Column(String(255),nullable=True)

    #relationships
    products = relationship("Product", back_populates="brand")

