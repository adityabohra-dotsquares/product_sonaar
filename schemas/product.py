from pydantic import BaseModel, Field, model_validator
from typing import List, Optional, Any
from decimal import Decimal
from datetime import datetime


class ProductSeoCreate(BaseModel):
    page_title: Optional[str] = None
    meta_description: Optional[str] = None
    meta_keywords: Optional[str] = None
    url_handle: Optional[str] = None
    canonical_url: Optional[str] = None


class VariantImageCreate(BaseModel):
    image_url: Optional[str] = None
    is_main: Optional[bool] = False
    image_order: Optional[int] = None
    video_url: Optional[str] = None


class VariantImageResponse(BaseModel):
    id: Optional[str] = None
    image_url: Optional[str] = None
    is_main: Optional[bool] = False
    image_order: Optional[int] = None
    video_url: Optional[str] = None

    class Config:
        from_attributes = True


# ---------- Image details ----------
class ProductImageResponse(BaseModel):
    id: Optional[str] = None
    url: Optional[str] = Field(alias="image_url")
    is_main: Optional[bool] = False
    image_order: Optional[int] = None
    video_url: Optional[str] = None

    class Config:
        from_attributes = True
        populate_by_name = True


# ---------- Product details ----------
class ProductListRequest(BaseModel):
    product_ids: List[str]


class ReviewResponse(BaseModel):
    id: str
    reviewer_name: Optional[str] = "Anonymous"
    rating: Optional[float] = 0.0
    comment: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


# ---------- Review Stats Response ----------


class ReviewStatsResponse(BaseModel):
    average_rating: float
    total_reviews: int
    five_star_count: int
    four_star_count: int
    three_star_count: int
    two_star_count: int
    one_star_count: int

    class Config:
        from_attributes = True


# ---------- Product Schemas ----------


class ProductBase(BaseModel):
    sku: str
    title: str
    unique_code: Optional[str] = None
    product_code: Optional[str] = None
    product_condition: Optional[str] = None
    fast_dispatch: Optional[bool] = None
    free_shipping: Optional[bool] = None
    description: str
    slug: Optional[str] = None
    price: Optional[Decimal] = None
    cost_price: Optional[Decimal] = None
    rrp_price: Optional[Decimal] = None
    status: str
    weight: Optional[Decimal] = None
    width: Optional[Decimal] = None
    height: Optional[Decimal] = None
    length: Optional[Decimal] = None
    unit: Optional[str] = None
    supplier: Optional[str] = None
    country_of_origin: Optional[str] = None
    ean: Optional[str] = None
    asin: Optional[str] = None
    mpn: Optional[str] = None
    category_id: str
    brand_id: str
    stock: int
    bundle_group_code: Optional[str] = None
    hs_code: Optional[str] = None
    tags: Optional[List[str]] = []
    key_features: Optional[str] = None
    is_battery_required: Optional[bool] = False
    precautionary_note: Optional[str] = None
    care_instructions: Optional[str] = None
    warranty: Optional[str] = None
    ships_from_location: Optional[str] = None
    handling_time_days: Optional[int] = None
    estimated_shipping_cost: Optional[Decimal] = None
    product_margin_percent: Optional[Decimal] = None
    product_margin_amount: Optional[Decimal] = None
    profit: Optional[Decimal] = None
    product_id_type: Optional[str] = None
    vendor_id: Optional[str] = None


# ---------------- ARCHIVE PRODUCT ----------------
class ProductArchiveRequest(BaseModel):
    product_ids: List[str]


# ---------------- NEW SCHEMAS FOR PRODUCT ----------------
class VariantAttributeCreate(BaseModel):
    name: str
    value: str


class ProductVariantCreate(BaseModel):
    title: str
    sku: str
    price: Decimal
    cost_price: Decimal
    ean: Optional[str] = None
    asin: Optional[str] = None
    mpn: Optional[str] = None
    rrp_price: Decimal
    stock: Optional[int] = None
    bundle_group_code: Optional[str] = None
    image_url: Optional[str] = None
    ships_from_location: Optional[str] = None
    handling_time_days: Optional[int] = None
    estimated_shipping_cost: Optional[Decimal] = None
    product_margin_percent: Optional[Decimal] = None
    product_margin_amount: Optional[Decimal] = None
    profit: Optional[Decimal] = None
    weight: Optional[Decimal] = None
    width: Optional[Decimal] = None
    height: Optional[Decimal] = None
    hs_code: Optional[str] = None
    length: Optional[Decimal] = None
    attributes: Optional[List[VariantAttributeCreate]] = Field(default_factory=list)


# ---------- Variant Attribute ----------
class VariantAttributeResponse(BaseModel):
    name: str
    value: str

    class Config:
        from_attributes = True


# ---------- Attribute ----------
class AttributeResponse(BaseModel):
    name: str
    value: str

    class Config:
        from_attributes = True


# ---------- Variant ----------
class VariantResponse(BaseModel):
    id: str
    sku: str
    title: str
    price: Decimal
    # cost_price: Optional[Decimal] = None
    stock: int
    length: Optional[Decimal] = None
    width: Optional[Decimal] = None
    height: Optional[Decimal] = None
    weight: Optional[Decimal] = None
    ships_from_location: Optional[str] = None
    handling_time_days: Optional[int] = None
    estimated_shipping_cost: Optional[Decimal] = None
    product_margin_percent: Optional[Decimal] = None
    product_margin_amount: Optional[Decimal] = None
    profit: Optional[Decimal] = None
    rrp_price: Optional[Decimal] = None
    attributes: List[AttributeResponse] = []
    images: List[VariantImageResponse] = []

    class Config:
        from_attributes = True


# ---------- Product ----------
class ProductResponse(BaseModel):
    id: str
    brand_name: Optional[str] = None
    brand_slug: Optional[str] = None
    category_name: Optional[str] = None
    category_slug: Optional[str] = None
    sku: str
    title: str
    unique_code: Optional[str] = None
    description: Optional[str] = None
    key_features: Optional[str] = None
    slug: str
    price: Optional[Decimal] = None
    cost_price: Optional[Decimal] = None
    rrp_price: Optional[Decimal] = None
    status: str
    weight: Optional[Decimal] = None
    length: Optional[Decimal] = None
    width: Optional[Decimal] = None
    height: Optional[Decimal] = None
    unit: Optional[str] = None
    tags: Optional[List[str]] = None
    supplier: Optional[str] = None
    country_of_origin: Optional[str] = None
    ean: Optional[str] = None
    asin: Optional[str] = None
    mpn: Optional[str] = None
    free_shipping: Optional[bool] = None
    fast_dispatch: Optional[bool] = None
    category_id: str
    brand_id: str
    stock: Optional[int] = None
    product_condition: Optional[str] = None
    fast_dispatch: Optional[bool] = None
    free_shipping: Optional[bool] = None
    promotion_name: Optional[str] = None
    discount_percentage: float = 0
    discounted_price: float = 0
    review_stats: Optional[ReviewStatsResponse] = None
    reviews: Optional[List[ReviewResponse]] = []
    variants: List[VariantResponse] = []
    is_battery_required: Optional[bool] = False
    precautionary_note: Optional[str] = None
    care_instructions: Optional[str] = None
    warranty: Optional[str] = None
    ships_from_location: Optional[str] = None
    handling_time_days: Optional[int] = None
    estimated_shipping_cost: Optional[Decimal] = None
    product_margin_percent: Optional[Decimal] = None
    product_margin_amount: Optional[Decimal] = None
    profit: Optional[Decimal] = None
    images: Optional[List[ProductImageResponse]] = []
    is_variant: bool = False
    variant_title: Optional[str] = None
    parent_product_id: Optional[str] = None
    vendor_id: Optional[str] = None
    sold_count: Optional[int] = 0
    sale_ends_at: Optional[datetime] = None

    class Config:
        from_attributes = True
        validate_by_name = True


# ---------- Category Response ----------
class FilterItem(BaseModel):
    attribute: str
    values: List[Any]


# ---------- Paginated Response ----------
class PaginatedProductResponse(BaseModel):
    page: int
    limit: int
    total: int
    pages: int
    data: List[ProductResponse]
    filters: Optional[List[FilterItem]] = []


class AttributeUpdate(BaseModel):
    name: str
    value: str
    price: Optional[Decimal] = None
    sku: Optional[str] = None


class VariantUpdate(BaseModel):
    id: Optional[str] = None  # existing variant id (for partial updates)
    sku: Optional[str] = None
    title: Optional[str] = None
    price: Optional[Decimal] = None
    cost_price: Decimal
    stock: int
    ean: Optional[str] = None
    bundle_group_code: Optional[str] = None
    ships_from_location: Optional[str] = None
    handling_time_days: Optional[int] = None
    estimated_shipping_cost: Optional[Decimal] = None
    product_margin_percent: Optional[Decimal] = None
    product_margin_amount: Optional[Decimal] = None
    profit: Optional[Decimal] = None
    rrp_price: Optional[Decimal] = None
    hs_code: Optional[str] = None
    width: Optional[Decimal] = None
    height: Optional[Decimal] = None
    length: Optional[Decimal] = None
    weight: Optional[Decimal] = None
    attributes: Optional[List[AttributeUpdate]] = Field(default_factory=list)
    images: Optional[List[VariantImageCreate]] = Field(default_factory=list)


class ProductUpdate(BaseModel):
    sku: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    slug: Optional[str] = None
    price: Optional[Decimal] = None
    cost_price: Optional[Decimal] = None
    hs_code: Optional[str] = None
    status: Optional[str] = None
    weight: Optional[Decimal] = None
    width: Optional[Decimal] = None
    height: Optional[Decimal] = None
    length: Optional[Decimal] = None
    unit: Optional[str] = None
    bundle_group_code: Optional[str] = None
    supplier: Optional[str] = None
    country_of_origin: Optional[str] = None
    product_condition: Optional[str] = None
    fast_dispatch: Optional[bool] = None
    free_shipping: Optional[bool] = None
    tags: Optional[List[str]] = None
    ean: Optional[str] = None
    asin: Optional[str] = None
    mpn: Optional[str] = None
    category_id: Optional[str] = None
    brand_id: Optional[str] = None
    stock: Optional[int] = None
    is_battery_required: Optional[bool] = None
    precautionary_note: Optional[str] = None
    care_instructions: Optional[str] = None
    warranty: Optional[str] = None
    rrp_price: Optional[Decimal] = None
    ships_from_location: Optional[str] = None
    handling_time_days: Optional[int] = None
    estimated_shipping_cost: Optional[Decimal] = None
    product_margin_percent: Optional[Decimal] = None
    product_margin_amount: Optional[Decimal] = None
    profit: Optional[Decimal] = None
    vendor_id: Optional[str] = None
    variants: Optional[List[VariantUpdate]] = Field(default_factory=list)


class ProductImageUpdate(BaseModel):
    image_url: Optional[str] = None
    is_main: Optional[bool] = False
    image_order: Optional[int] = None
    video_url: Optional[str] = None


class ProductUpdateWithImages(ProductUpdate):
    images: Optional[List[ProductImageUpdate]] = None
    seo: Optional[ProductSeoCreate] = None


class ProductStatusOut(BaseModel):
    id: str
    sku: str
    title: str
    status: str

    class Config:
        from_attributes = True


class ProductStatusUpdate(BaseModel):
    status: str


class ProductImageCreate(BaseModel):
    image_url: Optional[str] = None
    is_main: Optional[bool] = False
    image_order: Optional[int] = None
    video_url: Optional[str] = None


class ProductVariantResponseWithImage(BaseModel):
    id: str
    sku: str
    title: str
    price: Decimal
    hs_code: Optional[str] = None
    stock: int
    ean: Optional[str] = None
    bundle_group_code: Optional[str] = None
    cost_price: Optional[Decimal] = None
    ships_from_location: Optional[str] = None
    handling_time_days: Optional[int] = None
    estimated_shipping_cost: Optional[Decimal] = None
    product_margin_percent: Optional[Decimal] = None
    product_margin_amount: Optional[Decimal] = None
    profit: Optional[Decimal] = None
    rrp_price: Optional[Decimal] = None
    attributes: List[VariantAttributeResponse] = []
    # image_url: Optional[str] = None
    images: List[VariantImageResponse] = []

    class Config:
        from_attributes = True


class ProductResponseWithImages(BaseModel):
    id: str
    sku: str
    title: str
    unique_code: Optional[str] = None
    description: str
    slug: Optional[str] = None
    hs_code: Optional[str] = None
    product_code: Optional[str] = None
    price: Decimal
    cost_price: Optional[Decimal] = None
    rrp_price: Optional[Decimal] = None
    status: str
    bundle_group_code: Optional[str] = None
    product_condition: Optional[str] = None
    fast_dispatch: Optional[bool] = None
    free_shipping: Optional[bool] = None
    tags: Optional[List[str]] = None
    weight: Optional[Decimal] = None
    unit: Optional[str] = None
    supplier: Optional[str] = None
    country_of_origin: Optional[str] = None
    key_features: Optional[str] = None
    ean: Optional[str] = None
    asin: Optional[str] = None
    mpn: Optional[str] = None
    category_id: str
    brand_id: str
    stock: Optional[int] = None
    product_id_type: Optional[str] = None
    vendor_id: Optional[str] = None
    variants: List[ProductVariantResponseWithImage] = []
    images: List[ProductImageResponse] = []
    seo: Optional[ProductSeoCreate] = None

    class Config:
        from_attributes = True


class ProductVariantCreateWithImages(ProductVariantCreate):

    images: Optional[List[VariantImageCreate]] = Field(default_factory=list)


class ProductCreateWithVariantImages(ProductBase):
    variants: Optional[List[ProductVariantCreateWithImages]] = Field(
        default_factory=list
    )
    images: Optional[List[ProductImageCreate]] = Field(default_factory=list)
    seo: Optional[ProductSeoCreate] = None

    @model_validator(mode="after")
    def validate_prices(self):
        if not self.variants:
            if self.price is None:
                raise ValueError("Price is required when no variants are provided.")
            if self.cost_price is None:
                raise ValueError("Cost Price is required when no variants are provided.")
            if self.rrp_price is None:
                raise ValueError("RRP Price is required when no variants are provided.")
        return self


class ProductVariantResponseWithImages(ProductVariantResponseWithImage):
    images: List[VariantImageResponse] = []


class ProductResponseWithVariantImages(ProductResponseWithImages):
    brand_name: Optional[str] = None
    brand_slug: Optional[str] = None
    category_name: Optional[str] = None
    category_slug: Optional[str] = None
    variants: List[ProductVariantResponseWithImages] = []
    seo: Optional[ProductSeoCreate] = None


class ProductFilter(BaseModel):
    status: Optional[str] = None
    vendor: Optional[str] = None
    brand_id: Optional[str] = None
    ships_from: Optional[str] = None
    tag: Optional[str] = None
    title: Optional[str] = None
    sku: Optional[str] = None
    product_type: Optional[str] = None
    fast_dispatch: Optional[bool] = None
    free_shipping: Optional[bool] = None
    q: Optional[str] = None
    ean: Optional[str] = None
    asin: Optional[str] = None
    mpn: Optional[str] = None
    unique_code: Optional[str] = None
    bundle_group_code: Optional[str] = None
    condition: Optional[str] = None
    todays_special: Optional[bool] = None
    sales: Optional[bool] = None
    clearance: Optional[bool] = None
    limited_time_deal: Optional[bool] = None
    new_arrivals: Optional[bool] = None

    sort_by: str = Field(
        "newly_added",
        pattern="^(price_asc|price_desc|newly_added|oldest|stock_asc|stock_desc)$"
    )

    page: int = Field(1, ge=1)
    limit: int = Field(20, ge=1, le=200)
