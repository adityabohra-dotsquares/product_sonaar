from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime

class HomepageSectionBase(BaseModel):
    type: str = Field(..., description="Section type: hero_banner, product_slider, etc.")
    title: Optional[str] = None
    position: int = Field(0, description="Order on homepage")
    is_active: bool = True
    config: Dict[str, Any] = Field(..., description="Section-specific configuration")
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None

class HomepageSectionCreate(HomepageSectionBase):
    pass

class HomepageSectionUpdate(BaseModel):
    type: Optional[str] = None
    title: Optional[str] = None
    position: Optional[int] = None
    is_active: Optional[bool] = None
    config: Optional[Dict[str, Any]] = None
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None

class HomepageSectionRead(HomepageSectionBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True
