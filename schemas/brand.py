from pydantic import BaseModel, HttpUrl
from typing import Optional


class BrandStatusUpdate(BaseModel):
    is_active: bool


# Input for creating/updating a brand
class BrandCreate(BaseModel):
    name: str
    image_url: Optional[HttpUrl] = None
    logo_url: Optional[HttpUrl] = None


class BrandUpdate(BaseModel):
    name: Optional[str] = None
    slug: Optional[str] = None
    logo_url: Optional[HttpUrl] = None
    image_url: Optional[HttpUrl] = None


# Response schema
class BrandOut(BaseModel):
    id: str
    name: str
    slug: Optional[str] = None
    logo_url: Optional[str]
    image_url: Optional[str]
    is_active: Optional[bool] = None
    total_products: Optional[int] = None
    active_products: Optional[int] = None
    inactive_products: Optional[int] = None

    class Config:
        from_attributes = True


class BrandOutStatus(BaseModel):
    id: str
    name: str
    slug: Optional[str] = None
    logo_url: Optional[str]
    is_active: Optional[bool] = None

    class Config:
        from_attributes = True
