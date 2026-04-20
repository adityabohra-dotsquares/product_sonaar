"""
⚠️ NOTICE: This module is currently DISABLED ⚠️

These inventory viewing APIs are commented out in main.py.
The application currently uses the simple `product.stock` field for inventory management.

To re-enable ProductStock-based inventory tracking, uncomment the router registration in main.py:
- Line ~145: app.include_router(inventoryRouter, ...)

For now, query `Product.stock` directly instead of using ProductStock records.
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from typing import Optional, Annotated
from collections import defaultdict

from models.product import Product
from models.warehouse import Warehouse, ProductStock
from deps import get_db
from schemas.inventory import (
    ProductStockResponse,
    ProductStockListResponse,
    WarehouseAllocation,
)

router = APIRouter()


@router.get("/stock", response_model=ProductStockListResponse)
async def list_product_stock(
    db: Annotated[AsyncSession, Depends(get_db)],
    product_id: Annotated[Optional[str], Query(description="Filter by product ID")] = None,
    sku: Annotated[Optional[str], Query(description="Filter by product SKU")] = None,
    warehouse_id: Annotated[Optional[str], Query(description="Filter by warehouse ID")] = None,
):
    """
    List product stock levels across warehouses.
    
    Returns aggregated total stock and breakdown by warehouse for each product.
    Supports filtering by product_id, sku, or warehouse_id.
    """
    
    # Build query with joins
    query = (
        select(ProductStock)
        .join(Product, ProductStock.product_id == Product.id)
        .join(Warehouse, ProductStock.warehouse_id == Warehouse.id)
        .options(
            selectinload(ProductStock.product),
            selectinload(ProductStock.warehouse)
        )
    )
    
    # Apply filters
    if product_id:
        query = query.where(ProductStock.product_id == product_id)
    
    if sku:
        query = query.where(Product.sku == sku)
    
    if warehouse_id:
        query = query.where(ProductStock.warehouse_id == warehouse_id)
    
    # Execute query
    result = await db.execute(query)
    stock_records = result.scalars().all()
    
    # Group by product_id
    product_stocks = defaultdict(lambda: {
        "product_id": None,
        "sku": None,
        "product_name": None,
        "total_quantity": 0,
        "allocations": []
    })
    
    for stock in stock_records:
        product = stock.product
        warehouse = stock.warehouse
        
        product_data = product_stocks[product.id]
        
        # Set product info (only once per product)
        if product_data["product_id"] is None:
            product_data["product_id"] = product.id
            product_data["sku"] = product.sku
            product_data["product_name"] = product.title
        
        # Add to total quantity
        product_data["total_quantity"] += stock.quantity
        
        # Add warehouse allocation
        product_data["allocations"].append(
            WarehouseAllocation(
                warehouse_id=warehouse.id,
                warehouse_name=warehouse.name,
                warehouse_location=warehouse.location,
                quantity=stock.quantity
            )
        )
    
    # Convert to list of ProductStockResponse
    stocks = [
        ProductStockResponse(**data)
        for data in product_stocks.values()
    ]
    
    return ProductStockListResponse(
        stocks=stocks,
        count=len(stocks)
    )
