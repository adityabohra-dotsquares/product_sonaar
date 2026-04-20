from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class FeaturedBrandBase(BaseModel):
    brand_id: str
    position: Optional[int] = 0
    is_active: Optional[bool] = True

class FeaturedBrandCreate(FeaturedBrandBase):
    pass

class FeaturedBrandUpdate(BaseModel):
    position: Optional[int] = None
    is_active: Optional[bool] = None

class FeaturedBrandResponse(FeaturedBrandBase):
    id: str
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True
