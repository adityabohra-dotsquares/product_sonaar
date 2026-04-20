# models.py
from sqlalchemy import (
    Column,
    Integer,
    String,
    TIMESTAMP,
    func,
    Index,
    text,
    UniqueConstraint,
)
from database import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, ForeignKey, Boolean
import uuid


class Category(Base):
    __tablename__ = "categories"

    id = Column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        index=True,
    )
    name = Column(String(100), nullable=False)
    slug = Column(String(200), nullable=True, unique=True)
    category_code = Column(String(100), nullable=True, unique=True)
    parent_id = Column(String(36), ForeignKey("categories.id"), nullable=True)
    image_url = Column(String(255), nullable=True)
    icon_url = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    added_by = Column(String(255), nullable=True)
    updated_by = Column(String(255), nullable=True)

    parent = relationship(
        "Category",
        remote_side=[id],
        back_populates="subcategories",
    )

    subcategories = relationship(
        "Category",
        back_populates="parent",
    )

    products = relationship("Product", back_populates="category")
    categories_attributes = relationship(
        "CategoryAttribute",
        back_populates="category",
        cascade="all, delete",
        lazy="selectin",
    )
    __table_args__ = (
        Index("idx_category_name_fulltext", text("name"), mysql_prefix="FULLTEXT"),
        UniqueConstraint("name", "parent_id", name="uq_category_name_parent"),
    )

    def __repr__(self):
        return f"<Category(name={self.name}, parent_id={self.parent_id})>"


class CategoryAttribute(Base):
    __tablename__ = "categories_attributes"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(100), nullable=False)  # e.g. "RAM"

    category_id = Column(
        String(36),
        ForeignKey("categories.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    added_by = Column(String(255))
    updated_by = Column(String(255))

    # Relationship
    category = relationship("Category", back_populates="categories_attributes")
    values = relationship(
        "CategoryAttributeValue",
        back_populates="attribute",
        cascade="all, delete",
        lazy="selectin",
    )


class CategoryAttributeValue(Base):
    __tablename__ = "categories_attributes_values"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    value = Column(String(100), nullable=False)  # e.g. "16GB"
    attribute_id = Column(
        String(36),
        ForeignKey("categories_attributes.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())

    attribute = relationship("CategoryAttribute", back_populates="values")
