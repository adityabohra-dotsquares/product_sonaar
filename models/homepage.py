from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    JSON,
    func
)
from database import Base

class HomepageSection(Base):
    __tablename__ = "homepage_sections"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Section type: hero_banner, product_slider, category_slider, etc.
    type = Column(String(50), nullable=False)

    # Optional title shown on UI
    title = Column(String(100), nullable=True)

    # Order on homepage
    position = Column(Integer, nullable=False, index=True)

    # Enable / Disable section
    is_active = Column(Boolean, default=True, nullable=False)

    # Section-specific configuration
    config = Column(JSON, nullable=False)

    # Scheduling
    start_at = Column(DateTime, nullable=True)
    end_at = Column(DateTime, nullable=True)

    created_at = Column(
        DateTime,
        server_default=func.now(),
        nullable=False
    )
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False
    )
