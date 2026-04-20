from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class ReviewCreate(BaseModel):
    user_id: str = Field(..., json_schema_extra={"example": "John Doe"})
    order_id:str = Field(..., json_schema_extra={"example": "John Doe"})
    reviewer_name: str = Field(..., json_schema_extra={"example": "John Doe"})
    rating: float = Field(..., ge=1, le=5, json_schema_extra={"example": 5})
    comment: Optional[str] = Field(None, json_schema_extra={"example": "Excellent product!"})
    title: str
    images: Optional[List[str]] = None


class ReviewOut(BaseModel):
    id: str
    product_id: str
    reviewer_name: str
    rating: float
    comment: Optional[str]
    title: str
    sku: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


# ---------------- REVIEW UPDATE SCHEMA ----------------
class ReviewUpdate(BaseModel):
    reviewer_name: Optional[str] = Field(None, json_schema_extra={"example": "John Doe"})
    rating: Optional[float] = Field(None, ge=1, le=5, json_schema_extra={"example": 4})
    comment: Optional[str] = Field(None, json_schema_extra={"example": "Great product, fast delivery!"})
