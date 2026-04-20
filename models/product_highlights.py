from sqlalchemy import (
    Column,
    String,
    Integer,
    ForeignKey,
    TIMESTAMP,
    Boolean,
    func,
    Enum,
)
from sqlalchemy.orm import relationship
import uuid
from database import Base

class ProductHighlight(Base):
    __tablename__ = "product_highlights"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    title = Column(String(255), nullable=True)
    slug = Column(String(255), unique=True, nullable=False)
    type = Column(
        Enum(
            "Whats On Sale",
            "Today's Deal",
            "Todays Deals",
            "Trending Deals",
            "Top Rated",
            "Clearance",
            "Best Sellers",
            "New Releases",
            "Hot Deals",
            "Popular",
            name="highlight_type_enum",
        ),
        nullable=False,
        unique=True,
    )
    is_active = Column(Boolean, default=True)
    banner_image = Column(String(500), nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())

    # Relationship to ProductHighlightItem
    items = relationship(
        "ProductHighlightItem",
        back_populates="highlight",
        cascade="all, delete-orphan",
    )


class ProductHighlightItem(Base):
    __tablename__ = "product_highlight_items"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    highlight_id = Column(
        String(36),
        ForeignKey("product_highlights.id", ondelete="CASCADE"),
        nullable=False,
    )
    product_id = Column(
        String(36), ForeignKey("products.id", ondelete="CASCADE"), nullable=False
    )
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    highlight = relationship("ProductHighlight", back_populates="items")
    product = relationship("Product", back_populates="highlight_items")



# ALTER TABLE product_highlights MODIFY COLUMN type ENUM("Whats On Sale", "Today's Deal", "Todays Deals", "Trending Deals", "Clearance", "Best Sellers", "New Releases", "Hot Deals", "Popular") NOT NULL
# 