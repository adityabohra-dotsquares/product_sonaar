from pydantic import BaseModel, HttpUrl, Field, ConfigDict
from typing import Optional, List
from datetime import datetime
from decimal import Decimal
from typing import Literal

class CategoryBase(BaseModel):
    name: str
    slug: Optional[str] = None
    parent_id: Optional[str] = None
    image_url: Optional[str] = None
    icon_url: Optional[str] = None
    is_active: Optional[bool] = True
    category_code: Optional[str] = None

class CategoryUpdate(BaseModel):
    name: Optional[str] = None
    slug: Optional[str] = None
    parent_id: Optional[str] = None
    image_url: Optional[str] = None
    icon_url: Optional[str] = None
    category_code: Optional[str] = None

class CategoryAttributeValueCreate(BaseModel):
    value: str
    is_active: bool = True

class CategoryAttributeValueRead(CategoryAttributeValueCreate):
    id: str
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class CategoryAttributeCreate(BaseModel):
    name: str
    is_active: Optional[bool] = True
    values: List[CategoryAttributeValueCreate] = Field(default_factory=list)

class CategoryAttributeRead(CategoryAttributeCreate):
    id: str
    category_id: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    values: List[CategoryAttributeValueRead] = Field(default_factory=list)

    class Config:
        from_attributes = True

class CategoryRead(CategoryBase):
    id: str
    product_count: int = 0
    subcategories: List["CategoryRead"] = Field(default_factory=list)
    attributes: List[CategoryAttributeRead] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

CategoryRead.update_forward_refs()

class ProductVariantAttributeResponse(BaseModel):
    name: str
    value: str

    class Config:
        from_attributes = True

class ProductVariantResponse(BaseModel):
    id: str
    sku: str
    price: Decimal
    stock: int
    attributes: List[ProductVariantAttributeResponse] = []

    class Config:
        from_attributes = True

class ProductResponse(BaseModel):
    id: str
    sku: str
    title: str
    description: str
    slug: str
    price: Decimal
    cost_price: Decimal
    status: str
    stock: int
    weight: float
    unit: str
    variants: List[ProductVariantResponse] = []

    class Config:
        from_attributes = True

class CategoryWithProducts(BaseModel):
    id: str
    name: str
    image_url: Optional[str] = None
    icon_url: Optional[str] = None
    parent_id: Optional[str] = None
    promotion_name: Optional[str] = None
    discount_percentage: float = 0
    discounted_price: float = 0
    products: List[ProductResponse] = []

    class Config:
        from_attributes = True

class CategoryAttributeResponse(BaseModel):
    id: str
    name: str
    is_active: bool

    class Config:
        from_attributes = True

class SubCategoryResponse(BaseModel):
    id: str
    name: str
    is_active: bool
    attributes: List[CategoryAttributeResponse] = []

    class Config:
        from_attributes = True

class SubCategoryFilterResponse(BaseModel):
    parent_id: Optional[str]
    category_name: Optional[str]
    image_url: Optional[str]
    icon_url: Optional[str]
    created_by: Optional[str]
    updated_by: Optional[str]
    subcategories: List[SubCategoryResponse]

    class Config:
        from_attributes = True

class CategoryCreate(CategoryBase):
    attributes: List[CategoryAttributeCreate] = Field(default_factory=list)

class CategoryOutStatus(BaseModel):
    id: str
    name: str
    image_url: Optional[str]
    icon_url: Optional[str]
    is_active: bool

    class Config:
        from_attributes = True

class CategoryStatusUpdate(BaseModel):
    is_active: bool
