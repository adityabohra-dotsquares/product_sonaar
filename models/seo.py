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


class ProductSEO(Base):
    __tablename__ = "product_seo"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    product_id = Column(
        String(36),
        ForeignKey(
            "products.id",
            name="fk_product_seo_product_id",
            ondelete="CASCADE",
        ),
        unique=True,
        nullable=True,
    )

    page_title = Column(String(300), nullable=True)
    meta_description = Column(Text, nullable=True)
    meta_keywords = Column(Text, nullable=True)
    url_handle = Column(String(255), nullable=True, index=True)
    canonical_url = Column(String(255), nullable=True, index=True)

    product = relationship("Product", back_populates="seo")

class ProductMetafield(Base):
    __tablename__ = "product_metafields"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    product_id = Column(
        String(36),
        ForeignKey(
            "products.id",
            name="fk_product_metafields_product_id",
            ondelete="CASCADE",
        ),
        nullable=True,
    )

    namespace = Column(String(100), nullable=True)
    key = Column(String(150), nullable=True)
    value = Column(Text, nullable=True)
    value_type = Column(String(20), nullable=True, default="text")

    product = relationship("Product", back_populates="metafields")

    __table_args__ = (
        UniqueConstraint(
            "product_id",
            "namespace",
            "key",
            name="uq_product_metafield",
        ),
    )
