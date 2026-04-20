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
from sqlalchemy.orm import validates
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from models.seo import ProductSEO, ProductMetafield


class Product(Base):
    __tablename__ = "products"

    id = Column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        unique=True,
        index=True,
    )
    brand_id = Column(String(36), ForeignKey("brands.id"), nullable=False, index=True)
    category_id = Column(String(36), ForeignKey("categories.id"), nullable=False, index=True)
    vendor_id = Column(String(36), nullable=True)
    sku = Column(String(100), nullable=False, unique=True)
    title = Column(String(500), nullable=False)
    description = Column(Text, nullable=True)
    key_features = Column(Text, nullable=True)
    slug = Column(String(500), nullable=False, index=True)
    tags = Column(JSON, nullable=True)
    price = Column(DECIMAL(12, 2), nullable=True, index=True)
    cost_price = Column(DECIMAL(12, 2), nullable=True)
    status = Column(String(30), nullable=True, default="active", index=True)
    stock = Column(Integer, nullable=True, default=0)
    weight = Column(DECIMAL(10, 3), nullable=True)
    unit = Column(String(50), nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), index=True)
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    added_by = Column(String(255), nullable=True)
    updated_by = Column(String(255), nullable=True)
    supplier = Column(String(255), nullable=True)
    country_of_origin = Column(String(100), nullable=True)
    ean = Column(String(100), nullable=True, unique=True)
    asin = Column(String(100), nullable=True, unique=True)
    mpn = Column(String(100), nullable=True, unique=True)
    product_condition = Column(String(50), nullable=True)
    fast_dispatch = Column(Boolean, default=False)
    free_shipping = Column(Boolean, default=False)
    height = Column(Float, default=0)
    length = Column(Float, default=0)
    width = Column(Float, default=0)
    product_type = Column(String(100), nullable=True)
    product_id_type = Column(String(100), nullable=True)
    hs_code = Column(String(100), nullable=True)
    shipping_template = Column(String(100), nullable=True)
    category = relationship("Category", back_populates="products")
    brand = relationship("Brand", back_populates="products")
    # new cols added
    is_battery_required = Column(Boolean, default=False)
    precautionary_note = Column(Text, nullable=True)
    care_instructions = Column(Text, nullable=True)
    warranty = Column(Text, nullable=True)
    rrp_price = Column(DECIMAL(12, 2), nullable=True)
    ships_from_location = Column(String(255), nullable=True)
    handling_time_days = Column(Integer, nullable=True, default=2)
    estimated_shipping_cost = Column(DECIMAL(12, 2), nullable=True)
    product_margin_percent = Column(DECIMAL(12, 2), nullable=True)
    product_margin_amount = Column(DECIMAL(12, 2), nullable=True)
    profit = Column(DECIMAL(12, 2), nullable=True)
    variation_group_code = Column(String(255), nullable=True, index=True)
    product_code = Column(String(255), nullable=True, index=True)
    bundle_group_code = Column(String(255), nullable=True, index=True)
    unique_code = Column(String(10), unique=True, index=True, nullable=True)

    variants = relationship(
        "ProductVariant",
        back_populates="product",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    reviews = relationship(
        "Review",
        back_populates="product",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    warehouse_stocks = relationship(
        "ProductStock", back_populates="product", cascade="all, delete-orphan"
    )
    images = relationship(
        "ProductImage", back_populates="product", cascade="all, delete-orphan"
    )
    review_stats = relationship(
        "ReviewStats",
        back_populates="product",
        uselist=False,
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    recently_viewed_by = relationship(
        "RecentlyViewed", back_populates="product", cascade="all, delete-orphan"
    )
    stats = relationship(
        "ProductStats", back_populates="product", cascade="all, delete-orphan"
    )
    seo = relationship(
        "ProductSEO",
        uselist=False,
        back_populates="product",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    metafields = relationship(
    "ProductMetafield",
    back_populates="product",
    cascade="all, delete-orphan",
    passive_deletes=True,
)

    highlight_items = relationship(
        "ProductHighlightItem",
        back_populates="product",
        cascade="all, delete-orphan",
    )
    # stock_reservations = relationship(
    #     "StockReservation", back_populates="product", cascade="all, delete-orphan"
    # )

    @property
    def total_stock(self):
        if 'warehouse_stocks' in self.__dict__ and self.warehouse_stocks:
            return sum(stock.quantity for stock in self.warehouse_stocks)
        return 0

    @property
    def brand_name(self):
        if 'brand' in self.__dict__ and self.brand:
            return self.brand.name
        return None

    @property
    def brand_slug(self):
        if 'brand' in self.__dict__ and self.brand:
            return self.brand.slug
        return None

    @property
    def category_name(self):
        if 'category' in self.__dict__ and self.category:
            return self.category.name
        return None

    @property
    def category_slug(self):
        if 'category' in self.__dict__ and self.category:
            return self.category.slug
        return None

    @property
    def is_out_of_stock(self):
        return self.total_stock <= 0

    # @validates("price", "rrp_price")
    # def validate_price_cost(self, key, value):
    #     # `self.cost_price` or `self.price` may not be set yet; validate when both exist
    #     if key == "price":
    #         if (
    #             self.rrp_price is not None and value > self.rrp_price
    #         ):  # cost price should be less than price
    #             raise HTTPException(
    #                 status_code=400,
    #                 detail="Price cannot be greater than RRP price.",
    #             )
    #     if key == "rrp_price":
    #         if (
    #             self.price is not None and self.price > value
    #         ):  # price should be greater than cost price
    #             raise HTTPException(
    #                 status_code=400,
    #                 detail="RRP price cannot be less than price.",
    #             )

    #     return value


class ProductVariant(Base):
    __tablename__ = "product_variants"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    product_id = Column(
        String(36), ForeignKey("products.id", ondelete="CASCADE"), nullable=False, index=True
    )
    title = Column(String(500), nullable=False)
    sku = Column(String(100), nullable=False, unique=True)
    price = Column(DECIMAL(12, 2), nullable=True)
    cost_price = Column(DECIMAL(12, 2), nullable=True)
    stock = Column(Integer, nullable=True, default=0)
    image_url = Column(String(255), nullable=True)
    # new add cols
    ships_from_location = Column(String(255), nullable=True)
    handling_time_days = Column(Integer, nullable=True)
    estimated_shipping_cost = Column(DECIMAL(12, 2), nullable=True)
    product_margin_percent = Column(DECIMAL(12, 2), nullable=True)
    product_margin_amount = Column(DECIMAL(12, 2), nullable=True)
    profit = Column(DECIMAL(12, 2), nullable=True)
    rrp_price = Column(DECIMAL(12, 2), nullable=True)
    height = Column(Float, default=0)
    length = Column(Float, default=0)
    width = Column(Float, default=0)
    weight = Column(Float, default=0)
    description = Column(Text, nullable=True)
    key_features = Column(Text, nullable=True)
    slug = Column(String(500), nullable=True)
    tags = Column(JSON, nullable=True)
    status = Column(String(30), nullable=True, default="active")
    supplier = Column(String(255), nullable=True)
    country_of_origin = Column(String(100), nullable=True)
    ean = Column(String(100), nullable=True, unique=True)
    asin = Column(String(100), nullable=True, unique=True)
    mpn = Column(String(100), nullable=True, unique=True)
    product_condition = Column(String(50), nullable=True)
    fast_dispatch = Column(Boolean, default=False)
    free_shipping = Column(Boolean, default=False)
    product_type = Column(String(100), nullable=True)
    product_id_type = Column(String(100), nullable=True)
    hs_code = Column(String(100), nullable=True)
    shipping_template = Column(String(100), nullable=True)
    bundle_group_code = Column(String(255), nullable=True, index=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    is_battery_required = Column(Boolean, default=False)
    precautionary_note = Column(Text, nullable=True)
    care_instructions = Column(Text, nullable=True)
    warranty = Column(Text, nullable=True)

    product = relationship("Product", back_populates="variants")
    attributes = relationship(
        "Attribute", back_populates="variant", cascade="all, delete-orphan"
    )
    images = relationship(
        "ProductVariantImage",
        back_populates="variant",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    stocks = relationship(
        "ProductStock",
        back_populates="variant",
        cascade="all, delete-orphan",
    )
    @property
    def total_stock(self):
        if 'stocks' in self.__dict__ and self.stocks:
            return sum(stock.quantity for stock in self.stocks)
        return 0

    @property
    def brand_name(self):
        if 'product' in self.__dict__ and self.product:
            return self.product.brand_name
        return None

    @property
    def brand_slug(self):
        if 'product' in self.__dict__ and self.product:
            return self.product.brand_slug
        return None

    @property
    def category_name(self):
        if 'product' in self.__dict__ and self.product:
            return self.product.category_name
        return None

    @property
    def category_slug(self):
        if 'product' in self.__dict__ and self.product:
            return self.product.category_slug
        return None

    @property
    def is_out_of_stock(self):
        return self.total_stock <= 0

    @validates("price", "rrp_price")
    def validate_price_cost(self, key, value):
        # `self.cost_price` or `self.price` may not be set yet; validate when both exist
        if key == "price":
            if (
                self.rrp_price is not None
                and value is not None
                and value > self.rrp_price
            ):  # price should be less than rrp price
                raise HTTPException(
                    status_code=400,
                    detail="Price cannot be greater than RRP price.",
                )
        if key == "rrp_price":
            if (
                self.price is not None and value is not None and self.price > value
            ):  # rrp price should be greater than price
                raise HTTPException(
                    status_code=400,
                    detail="RRP price cannot be less than price.",
                )

        return value

    @validates("ean")
    def clean_ean(self, key, value):
        if value is None:
            return None

        value = value.strip()
        return value if value != "" else None

class Attribute(Base):
    __tablename__ = "attributes"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)  # e.g., "color", "size"
    value = Column(String(255), nullable=False)  # e.g., "red", "XL"
    variant_id = Column(
        String(36),
        ForeignKey("product_variants.id", ondelete="CASCADE"),
        nullable=False,
    )

    variant = relationship("ProductVariant", back_populates="attributes")


class ProductImage(Base):
    __tablename__ = "product_images"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    added_by = Column(String(255), nullable=True)
    updated_by = Column(String(255), nullable=True)
    product_id = Column(String(36), ForeignKey("products.id", ondelete="CASCADE"))
    image_url = Column(String(255), nullable=True)
    video_url = Column(String(255), nullable=True)
    is_main = Column(Boolean, default=False)
    image_order = Column(Integer, default=0)
    product = relationship("Product", back_populates="images")


class ProductVariantImage(Base):
    __tablename__ = "product_variant_images"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    added_by = Column(String(255), nullable=True)
    updated_by = Column(String(255), nullable=True)

    variant_id = Column(
        String(36),
        ForeignKey("product_variants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    image_url = Column(String(255), nullable=True)
    video_url = Column(String(255), nullable=True)
    is_main = Column(Boolean, default=False)
    image_order = Column(Integer, default=0)
    variant = relationship("ProductVariant", back_populates="images")


class SearchHistory(Base):
    __tablename__ = "search_history"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), index=True, nullable=True)
    query = Column(String(255), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())


class RecentlyViewed(Base):
    __tablename__ = "recently_viewed"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), nullable=True, index=True)  # nullable for guest sessions
    product_id = Column(
        String(36), ForeignKey("products.id", ondelete="CASCADE"), nullable=False
    )
    session_id = Column(String(255), nullable=False)
    viewed_at = Column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    # user = relationship("User", back_populates="recently_viewed_products")
    product = relationship("Product", back_populates="recently_viewed_by")


class ProductStats(Base):
    __tablename__ = "product_stats"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    product_id = Column(
        String(36), ForeignKey("products.id", ondelete="CASCADE"), nullable=False
    )
    views = Column(Integer, default=0, nullable=True)
    orders = Column(Integer, default=0, nullable=True)
    added_to_cart = Column(Integer, default=0, nullable=True)
    week_start = Column(TIMESTAMP(timezone=True), nullable=True)
    month_start = Column(TIMESTAMP(timezone=True), nullable=True)
    created_at = Column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )

    # optional relationship for easy access
    product = relationship("Product", back_populates="stats")

    __table_args__ = (
        UniqueConstraint(
            "product_id", "week_start", "month_start", name="uq_product_stats_period"
        ),
    )
