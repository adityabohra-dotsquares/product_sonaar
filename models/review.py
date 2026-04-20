import uuid
from sqlalchemy import (
    Column,
    String,
    Text,
    ForeignKey,
    Integer,
    TIMESTAMP,
    func,
    Float,
    Boolean,
    JSON
)
from sqlalchemy.orm import relationship
from database import Base


class Review(Base):
    __tablename__ = "reviews"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    product_id = Column(String(36), ForeignKey("products.id"), nullable=False)
    variant_id = Column(String(36), ForeignKey("product_variants.id"), nullable=True)
    reviewer_name = Column(String(100), nullable=False)  # TODO ADD REVIEWER ID
    
    variant = relationship("ProductVariant", backref="reviews")
    rating = Column(Float, nullable=True)
    comment = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())

    product_identifier = Column(String(36), nullable=False)
    user_id = Column(String(36), nullable=True)  # customer
    vendor_id = Column(String(36), nullable=True)  # vendor/brand
    order_id = Column(String(36), nullable=True)  # delivery proof
    review_type = Column(String(20), nullable=False)  # customer | vendor | brand
    is_verified_purchase = Column(Boolean, default=False)
    is_official = Column(Boolean, default=False)
    product = relationship("Product", back_populates="reviews")
    title = Column(String(225), nullable=False)  
    images = Column(JSON, nullable=True)  

    @property
    def sku(self):
        # Prevent lazy loading during serialization by checking __dict__
        if 'variant' in self.__dict__ and self.variant:
            return self.variant.sku
        if 'product' in self.__dict__ and self.product:
            return self.product.sku
        return None


class ReviewStats(Base):
    __tablename__ = "review_stats"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    product_id = Column(
        String(36), ForeignKey("products.id", ondelete="CASCADE"), nullable=False
    )
    average_rating = Column(Float, default=0.0)
    total_reviews = Column(Integer, default=0)
    five_star_count = Column(Integer, default=0, nullable=True)
    four_star_count = Column(Integer, default=0, nullable=True)
    three_star_count = Column(Integer, default=0, nullable=True)
    two_star_count = Column(Integer, default=0, nullable=True)
    one_star_count = Column(Integer, default=0, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    product = relationship("Product", back_populates="review_stats")
