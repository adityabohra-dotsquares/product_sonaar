from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from schemas.product import ProductResponse, PaginatedProductResponse

class ProductHighlightBase(BaseModel):
    title: Optional[str] = None
    type: str # Enum validation handled by DB or usage
    is_active: bool = True
    banner_image: Optional[str] = None

class ProductHighlightCreate(ProductHighlightBase):
    pass

class ProductHighlightUpdate(ProductHighlightBase):
    type: Optional[str] = None
    is_active: Optional[bool] = None
    banner_image: Optional[str] = None

class ProductHighlightOut(ProductHighlightBase):
    id: str
    slug: str
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class ProductHighlightItemCreate(BaseModel):
    product_id: str

class ProductHighlightItemOut(BaseModel):
    id: str
    highlight_id: str
    product_id: str
    product: Optional[ProductResponse] = None

    class Config:
        from_attributes = True

class ProductHighlightWithItemsOut(ProductHighlightOut):
    items: List[ProductHighlightItemOut] = []

class ProductHighlightWithPaginatedItemsOut(ProductHighlightOut):
    products: PaginatedProductResponse

