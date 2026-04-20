from pydantic import BaseModel, Field
from typing import List, Optional, Literal


class WarehouseBase(BaseModel):
    product_id: str
    warehouse_id: str
    quantity: int
    name: Optional[str] = None
    location: Optional[str] = None


class WarehouseStockCreate(BaseModel):
    product_id: str
    quantity: int


class WarehouseCreate(BaseModel):
    name: str
    location: str | None = None
    stocks: List[WarehouseStockCreate] = []


class WarehouseStatusUpdate(BaseModel):
    status: str


class SetStockRequest(BaseModel):
    product_id: str
    warehouse_name: str
    quantity: int = Field(ge=0)

    # optional — only if this is variant stock
    variant_id: Optional[str] = None

    # optional — only if using name+location uniqueness
    location: Optional[str] = None


class InventoryBatchItem(BaseModel):
    product_id: str
    quantity: int = Field(gt=0, description="Quantity to lock/release/restock")
    reference_id: Optional[str] = Field(None, description="Order ID, Transfer ID, etc. (required for lock)")
    reference_type: str = Field(default="order", description="Type: order, transfer, manual")
    notes: Optional[str] = Field(None, description="Optional notes about this operation")
    sku: Optional[str] = None  # For reference only
    warehouse_id: Optional[str] = None  # Ignored in current implementation


class InventoryBatchRequest(BaseModel):
    action: Literal["lock", "release", "restock"]
    items: List[InventoryBatchItem]

