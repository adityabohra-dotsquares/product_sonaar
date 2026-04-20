from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from models.warehouse import ProductStock, Warehouse
from repositories.warehouse.warehouse_repository import WarehouseRepository


async def set_stock(
    db: AsyncSession,
    *,
    product_id: str,
    warehouse_name: str,
    quantity: int,
    variant_id: str | None = None,
    location: str | None = None,
    added_by: str | None = None,
):
    """
    Create/update stock by warehouse name (case-insensitive)
    and optional location using WarehouseRepository.
    """

    if not warehouse_name:
        return None

    repo = WarehouseRepository(db)
    
    # --- case-insensitive warehouse search ---
    warehouse = await repo.get_by_name(warehouse_name, location)

    if not warehouse:
        raise HTTPException(
            status_code=404,
            detail=f"Warehouse '{warehouse_name}' not found"
            + (f" (location: {location})" if location else ""),
        )

    # --- find or create stock row ---
    stock = await repo.get_product_stock(product_id, warehouse.id, variant_id)

    if stock:
        stock.quantity = quantity
        stock.updated_by = added_by
    else:
        stock = ProductStock(
            product_id=product_id,
            warehouse_id=warehouse.id,
            variant_id=variant_id,
            quantity=quantity,
            added_by=added_by,
        )
        repo.add(stock)

    await repo.flush()
    return stock


async def validate_warehouse(db: AsyncSession, warehouse_name: str):
    """
    Ensure warehouse exists (case-insensitive).
    """
    repo = WarehouseRepository(db)
    warehouse = await repo.get_by_name(warehouse_name)

    if warehouse:
        return warehouse.name

    raise HTTPException(
        status_code=400,
        detail=f"Warehouse '{warehouse_name}' does not exist.",
    )
