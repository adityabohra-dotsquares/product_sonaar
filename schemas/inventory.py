from pydantic import BaseModel
from typing import List, Optional


class WarehouseAllocation(BaseModel):
    warehouse_id: str
    warehouse_name: str
    warehouse_location: Optional[str] = None
    quantity: int


class ProductStockResponse(BaseModel):
    product_id: str
    sku: str
    product_name: str
    total_quantity: int
    allocations: List[WarehouseAllocation]


class ProductStockListResponse(BaseModel):
    stocks: List[ProductStockResponse]
    count: int
