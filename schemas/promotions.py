# schemas/promotion.py
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, model_validator


class PromotionCreate(BaseModel):
    name: str
    description: Optional[str] = None
    max_discount_amount: Optional[float] = None
    offer_type: str
    reference_id: str
    discount_type: str  # "percentage" | "fixed"
    discount_percentage: float | None = None
    discount_value: float | None = None
    start_date: datetime
    end_date: datetime

    @model_validator(mode="after")
    def validate_discount(self):
        dtype = self.discount_type
        percent = self.discount_percentage
        value = self.discount_value

        if dtype == "percentage":
            if percent is None or percent <= 0:
                raise ValueError(
                    "percentage promotions require discount_percentage > 0"
                )
            self.discount_value = 0

        elif dtype == "fixed":
            if value is None or value <= 0:
                raise ValueError("fixed promotions require discount_value > 0")
            self.discount_percentage = None

        else:
            raise ValueError("discount_type must be 'percentage' or 'fixed'")

        if self.start_date >= self.end_date:
            raise ValueError("start_date must be before end_date")

        return self


class PromotionResponse(BaseModel):
    id: str
    offer_name: str
    description: Optional[str]
    discount_value: float
    max_discount_amount: Optional[float] = None
    discount_percentage: Optional[float]
    offer_type: str
    status: str
    reference_id: str
    start_date: datetime
    end_date: Optional[datetime]
    created_at: datetime

    class Config:
        from_attributes = True


class ProductIdRequest(BaseModel):
    product_ids: List[str]


class PaginatedPromotions(BaseModel):
    total: int
    page: int
    limit: int
    pages: int
    data: list[PromotionResponse]
