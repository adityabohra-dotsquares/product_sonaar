from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
from models.warehouse import Warehouse, ProductStock
from repositories.base import BaseRepository

class WarehouseRepository(BaseRepository[Warehouse]):
    def __init__(self, db: AsyncSession):
        super().__init__(db, Warehouse)

    async def get_by_name(self, name: str, location: Optional[str] = None) -> Optional[Warehouse]:
        query = select(Warehouse).where(func.lower(Warehouse.name) == name.lower())
        if location:
            query = query.where(func.lower(Warehouse.location) == location.lower())
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def get_product_stock(self, product_id: str, warehouse_id: str, variant_id: Optional[str] = None) -> Optional[ProductStock]:
        stmt = select(ProductStock).where(
            ProductStock.product_id == product_id,
            ProductStock.warehouse_id == warehouse_id,
            ProductStock.variant_id == variant_id
        )
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def list_warehouses(self, is_active: bool = True) -> List[Warehouse]:
        stmt = select(Warehouse)
        if is_active:
            stmt = stmt.where(Warehouse.status == "active")
        result = await self.db.execute(stmt)
        return result.scalars().all()
