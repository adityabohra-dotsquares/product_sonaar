from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class ReturnPolicyBase(BaseModel):
    scope_type: str
    scope_id: Optional[str] = None
    country_code: Optional[str] = None
    days: int
    restocking_fee_pct: int = 0
    text: str
    status: str = "active"
    priority: int = 0
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None


class ReturnPolicyCreate(ReturnPolicyBase):
    pass


class ReturnPolicyUpdate(ReturnPolicyBase):
    pass


class ReturnPolicyOut(ReturnPolicyBase):
    id: str

    class Config:
        from_attributes = True
