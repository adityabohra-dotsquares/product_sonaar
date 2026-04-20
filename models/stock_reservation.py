import uuid
from sqlalchemy import Column, String, Integer, DateTime, Text, ForeignKey, func
from sqlalchemy.orm import relationship
from database import Base


class StockReservation(Base):
    """
    Tracks stock reservations/locks for orders and other operations.
    
    This allows tracking WHO locked stock, WHEN, and for WHAT purpose,
    providing full audit trail and preventing overselling.
    """
    __tablename__ = "stock_reservations"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    product_id = Column(
        String(36), 
        # ForeignKey("products.id", ondelete="CASCADE"),  <-- REMOVED to allow Variant IDs
        nullable=False,
        index=True
    )
    reference_id = Column(String(100), nullable=False, index=True)  # order_id, transfer_id, etc.
    reference_type = Column(String(50), nullable=False, default="order")  # 'order', 'transfer', 'manual'
    quantity = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False, default="active", index=True)  # 'active', 'released', 'fulfilled'
    notes = Column(Text, nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=True, index=True) # <-- Added expiration
    released_at = Column(DateTime(timezone=True), nullable=True)
    created_by = Column(String(255), nullable=True)
    released_by = Column(String(255), nullable=True)
    
    # Relationship
    # product = relationship("Product", back_populates="stock_reservations") # <-- Disabled due to loose foreign key
