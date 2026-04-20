"""
⚠️ NOTICE: This module is currently DISABLED ⚠️

These warehouse and ProductStock management APIs are commented out in main.py.
The application currently uses the simple `product.stock` field for inventory management.

To re-enable multi-warehouse inventory tracking, uncomment the router registration in main.py:
- Line ~111: app.include_router(warehouseRouter, ...)

For now, all stock operations use the `Product.stock` column directly.
"""

from datetime import datetime, timedelta, timezone
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func
from models.product import Product
from models.warehouse import Warehouse, ProductStock
from deps import get_db
from schemas.warehouse import (
    WarehouseBase,
    WarehouseCreate,
    WarehouseStockCreate,
    WarehouseStatusUpdate,
    InventoryBatchRequest,
)
from fastapi import Query
from sqlalchemy.orm import selectinload
from models.product import Product, ProductVariant
from service.warehouse import set_stock, validate_warehouse
from schemas.warehouse import SetStockRequest
from utils.activity_logger import log_activity

router = APIRouter()

# ============================================================================
# OLD WAREHOUSE/PRODUCTSTOCK ENDPOINTS - COMMENTED OUT
# These endpoints used the multi-warehouse ProductStock system
# Only the batch endpoint below is active and uses product.stock
# ============================================================================

# @router.post("/create-warehouse", status_code=status.HTTP_201_CREATED)
# async def create_warehouse(
#     warehouse: WarehouseCreate, db: AsyncSession = Depends(get_db)
# ):
#     # Check if warehouse already exists
#     existing = await db.execute(
#         select(Warehouse).where(
#             Warehouse.name == warehouse.name, Warehouse.location == warehouse.location
#         )
#     )
#     existing_warehouse = existing.scalar_one_or_none()
#     if existing_warehouse:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail=f"Warehouse '{warehouse.name} and {warehouse.location}' already exists.",
#         )
#
#     # Create warehouse
#     new_warehouse = Warehouse(name=warehouse.name, location=warehouse.location)
#     db.add(new_warehouse)
#     await db.flush()  # Get new_warehouse.id before adding stocks
#     await log_activity(
#         db,
#         entity_type="warehouse",
#         entity_id=new_warehouse.id,
#         action="create",
#         details={"name": new_warehouse.name, "location": new_warehouse.location},
#         performed_by="admin"
#     )
#
#     await db.commit()
#     await db.refresh(new_warehouse)
#
#     return {
#         "message": "Warehouse created successfully",
#         "warehouse": {
#             "id": new_warehouse.id,
#             "name": new_warehouse.name,
#             "location": new_warehouse.location,
#         },
#     }


# @router.post("/update-warehouse-details")
# async def update_stock(
#     stockdata: WarehouseBase,
#     db: AsyncSession = Depends(get_db),
# ):
#     # 1️⃣ Check if product exists
#     product_result = await db.execute(
#         select(Product).where(Product.id == stockdata.product_id)
#     )
#     product = product_result.scalar_one_or_none()
#     if not product:
#         raise HTTPException(status_code=404, detail="Product not found")
#
#     # 2️⃣ Check if warehouse exists
#     warehouse_result = await db.execute(
#         select(Warehouse).where(Warehouse.id == stockdata.warehouse_id)
#     )
#     warehouse = warehouse_result.scalar_one_or_none()
#     if not warehouse:
#         raise HTTPException(status_code=404, detail="Warehouse not found")
#
#     # 🔹 Optional: Update warehouse details if provided
#     if stockdata.name is not None:
#         warehouse.name = stockdata.name
#     if stockdata.location is not None:
#         warehouse.location = stockdata.location
#
#     # 3️⃣ Check if stock record exists
#     stock_result = await db.execute(
#         select(ProductStock).where(
#             ProductStock.product_id == stockdata.product_id,
#             ProductStock.warehouse_id == stockdata.warehouse_id,
#         )
#     )
#     stock = stock_result.scalar_one_or_none()
#
#     # 4️⃣ Update or create stock record
#     if stock:
#         stock.quantity = stockdata.quantity
#     else:
#         stock = ProductStock(
#             product_id=stockdata.product_id,
#             warehouse_id=stockdata.warehouse_id,
#             quantity=stockdata.quantity,
#         )
#         db.add(stock)
#
#     # 5️⃣ Commit all changes
#     await log_activity(
#         db,
#         entity_type="warehouse",
#         entity_id=warehouse.id,
#         action="update",
#         details={"name": warehouse.name, "location": warehouse.location, "stock_updated": True},
#         performed_by="admin"
#     )
#     await db.commit()
#     await db.refresh(stock)
#
#     return {
#         "message": "Stock updated successfully",
#         "data": {
#             "product_id": stockdata.product_id,
#             "warehouse_id": stockdata.warehouse_id,
#             "quantity": stockdata.quantity,
#             "warehouse_name": warehouse.name,
#             "warehouse_location": warehouse.location,
#         },
#     }


# @router.get("/list-warehouses")
# async def list_warehouses(
#     db: AsyncSession = Depends(get_db),
#     status: str | None = Query(
#         "active",
#         description="Filter warehouses by status: 'active', 'inactive', or 'all'",
#     ),
# ):
#
#     allowed_status = ["active", "inactive", "all"]
#     if status.lower() not in allowed_status:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail=f"Invalid status filter. Choose from {allowed_status}.",
#         )
#
#     # 👇 load products AND variants
#     query = select(Warehouse).options(
#         selectinload(Warehouse.stocks).selectinload(ProductStock.product),
#         selectinload(Warehouse.stocks).selectinload(ProductStock.variant),
#     )
#
#     if status.lower() != "all":
#         query = query.where(Warehouse.status == status.lower())
#
#     result = await db.execute(query)
#     warehouses = result.scalars().unique().all()
#
#     warehouse_data = []
#
#     for warehouse in warehouses:
#         stock_items = []
#         total_quantity = 0
#
#         for stock in warehouse.stocks:
#
#             product = stock.product
#             variant = stock.variant  # 👈 may be None
#
#             if not product:
#                 continue
#
#             stock_items.append(
#                 {
#                     "product_id": product.id,
#                     "product_name": product.title,
#                     "product_sku": product.sku,
#                     # 👇 show variant details only when present
#                     "variant_id": variant.id if variant else None,
#                     "variant_title": variant.title if variant else None,
#                     "variant_sku": variant.sku if variant else None,
#                     "quantity": stock.quantity,
#                 }
#             )
#
#             total_quantity += stock.quantity
#
#         warehouse_data.append(
#             {
#                 "id": warehouse.id,
#                 "name": warehouse.name,
#                 "location": warehouse.location,
#                 "status": warehouse.status,
#                 "total_quantity": total_quantity,
#                 "products": stock_items,
#             }
#         )
#
#     return {"warehouses": warehouse_data, "count": len(warehouse_data)}


# @router.put(
#     "/update-status/{warehouse_id}",
#     status_code=status.HTTP_200_OK,
# )
# async def update_warehouse_status(
#     warehouse_id: str,
#     data: WarehouseStatusUpdate,
#     db: AsyncSession = Depends(get_db),
# ):
#     # Validate status input
#     if data.status.lower() not in ["active", "inactive"]:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail="Invalid status. Allowed values are 'active' or 'inactive'.",
#         )
#
#     # Find the warehouse
#     result = await db.execute(select(Warehouse).where(Warehouse.id == warehouse_id))
#     warehouse = result.scalar_one_or_none()
#
#     if not warehouse:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND,
#             detail=f"Warehouse with ID '{warehouse_id}' not found.",
#         )
#
#     # Update status
#     warehouse.status = data.status.lower()
#     await db.commit()
#     await db.refresh(warehouse)
#
#     return {
#         "id": warehouse.id,
#         "name": warehouse.name,
#         "status": warehouse.status,
#         "message": f"Warehouse status updated to '{warehouse.status}'.",
#     }


# @router.delete(
#     "/delete-warehouse/{warehouse_id}",
#     status_code=status.HTTP_200_OK,
# )
# async def delete_warehouse(
#     warehouse_id: str,
#     db: AsyncSession = Depends(get_db),
# ):
#     # Find the warehouse
#     result = await db.execute(select(Warehouse).where(Warehouse.id == warehouse_id))
#     warehouse = result.scalar_one_or_none()
#
#     if not warehouse:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND,
#             detail="Warehouse not found.",
#         )
#
#     # Delete the warehouse
#     await db.delete(warehouse)
#     await db.commit()
#
#     return {"message": "Warehouse deleted successfully."}


# @router.post("/set-update-stock")
# async def set_product_stock(
#     body: SetStockRequest,
#     db: AsyncSession = Depends(get_db),
# ):
#     # ---- validate product exists ----
#     result = await db.execute(select(Product).where(Product.id == body.product_id))
#     product = result.scalar_one_or_none()
#
#     if not product:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND,
#             detail="Product not found.",
#         )
#     # ---- validate warehouse exists ----
#     await validate_warehouse(db, body.warehouse_name)
#     # ---- if variant provided, validate it belongs to the product ----
#     if body.variant_id:
#         vr = await db.execute(
#             select(ProductVariant).where(
#                 ProductVariant.id == body.variant_id,
#                 ProductVariant.product_id == body.product_id,
#             )
#         )
#         variant = vr.scalar_one_or_none()
#
#         if not variant:
#             raise HTTPException(
#                 status_code=400,
#                 detail="Variant does not belong to this product.",
#             )
#
#     # ---- do the stock update ----
#     stock = await set_stock(
#         db,
#         product_id=body.product_id,
#         warehouse_name=body.warehouse_name,
#         quantity=body.quantity,
#         variant_id=body.variant_id,
#         location=body.location,
#         added_by="system",
#     )
#
#     await db.commit()
#
#     return {
#         "message": "Stock updated successfully.",
#         "product_id": body.product_id,
#         "variant_id": body.variant_id,
#         "warehouse_name": body.warehouse_name,
#         "quantity": stock.quantity,
#     }


# ============================================================================
# ACTIVE ENDPOINT - Uses simple product.stock field
# ============================================================================


@router.put(
    "/inventory/batch",
    status_code=status.HTTP_200_OK,
    responses={
        400: {"description": "Validation error (missing reference_id or insufficient stock)."},
        404: {"description": "Product, Variant, or Reservation not found."},
    },
)
async def batch_inventory_operation(
    payload: InventoryBatchRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Batch Lock/Release/Restock inventory with proper reservation tracking.
    Supports both Product and ProductVariant IDs.
    
    Actions:
    - lock: Creates StockReservation record (requires reference_id)
    - release: Updates StockReservation to 'released' status
    - restock: Increments product.stock or variant.stock (physical inventory)
    
    Stock Calculation:
    - target.stock = total physical inventory
    - available_stock = target.stock - SUM(active reservations)
    """
    from models.stock_reservation import StockReservation
    
    results = []
    
    for item in payload.items:
        # Smart Lookup: Try Product first, then Variant
        target_object = None
        target_type = None
        
        # 1. Try Product
        result = await db.execute(
            select(Product).where(Product.id == item.product_id)
        )
        product = result.scalar_one_or_none()
        
        if product:
            target_object = product
            target_type = "product"
        else:
            # 2. Try Variant
            result = await db.execute(
                select(ProductVariant).where(ProductVariant.id == item.product_id)
            )
            variant = result.scalar_one_or_none()
            if variant:
                target_object = variant
                target_type = "variant"
        
        if not target_object:
            raise HTTPException(
                status_code=404,
                detail=f"Product or Variant with ID '{item.product_id}' not found"
            )
        
        sku = target_object.sku
        
        # Perform the action
        if payload.action == "lock":
            # Validate reference_id is provided
            if not item.reference_id:
                raise HTTPException(
                    status_code=400,
                    detail="reference_id is required for lock operations (e.g., order_id)"
                )
            
            # Calculate current available stock
            # Note: We query by product_id (which now holds either Product ID or Variant ID)
            reserved_result = await db.execute(
                select(func.sum(StockReservation.quantity))
                .where(
                    StockReservation.product_id == item.product_id,
                    StockReservation.status == 'active'
                )
            )
            reserved_qty = reserved_result.scalar() or 0
            available_stock = target_object.stock - reserved_qty
            
            # Validate sufficient available stock
            if available_stock < item.quantity:
                raise HTTPException(
                    status_code=400,
                    detail=f"Insufficient available stock for {target_type} {sku}. "
                           f"Available: {available_stock}, Requested: {item.quantity} "
                           f"(Total: {target_object.stock}, Reserved: {reserved_qty})"
                )
            
            # Create reservation record
            # Default expiration: 30 minutes from now
            expires_at = datetime.now(timezone.utc) + timedelta(minutes=30)
            
            reservation = StockReservation(
                product_id=item.product_id, # Stores either Product ID or Variant ID
                reference_id=item.reference_id,
                reference_type=item.reference_type,
                quantity=item.quantity,
                status='active',
                notes=item.notes,
                created_by='system',
                expires_at=expires_at
            )
            db.add(reservation)
            
            results.append({
                "product_id": item.product_id,
                "type": target_type,
                "sku": sku,
                "action": payload.action,
                "quantity": item.quantity,
                "reference_id": item.reference_id,
                "reference_type": item.reference_type,
                "total_stock": target_object.stock,
                "previously_reserved": reserved_qty,
                "newly_reserved": item.quantity,
                "available_after": available_stock - item.quantity,
                "reservation_id": reservation.id
            })
            
        elif payload.action == "release":
            # Validate reference_id is provided
            if not item.reference_id:
                raise HTTPException(
                    status_code=400,
                    detail="reference_id is required for release operations"
                )
            
            # Find active reservation(s) for this reference AND specific product/variant ID
            reservations_result = await db.execute(
                select(StockReservation)
                .where(
                    StockReservation.product_id == item.product_id,
                    StockReservation.reference_id == item.reference_id,
                    StockReservation.status == 'active'
                )
            )
            reservations = reservations_result.scalars().all()
            
            if not reservations:
                raise HTTPException(
                    status_code=404,
                    detail=f"No active reservation found for {target_type} {item.product_id} "
                           f"with reference_id '{item.reference_id}'"
                )
            
            # Update reservation status
            released_qty = 0
            released_ids = []
            for reservation in reservations:
                reservation.status = 'released'
                reservation.released_at = func.now()
                reservation.released_by = 'system'
                released_qty += reservation.quantity
                released_ids.append(reservation.id)
            
            # Calculate available stock after release
            reserved_result = await db.execute(
                select(func.sum(StockReservation.quantity))
                .where(
                    StockReservation.product_id == item.product_id,
                    StockReservation.status == 'active'
                )
            )
            reserved_qty = reserved_result.scalar() or 0
            available_stock = target_object.stock - reserved_qty
            
            results.append({
                "product_id": item.product_id,
                "type": target_type,
                "sku": sku,
                "action": payload.action,
                "reference_id": item.reference_id,
                "released_quantity": released_qty,
                "reservation_ids": released_ids,
                "total_stock": target_object.stock,
                "still_reserved": reserved_qty,
                "available_after": available_stock
            })
            
        elif payload.action == "restock":
            # Add physical stock
            original_stock = target_object.stock
            target_object.stock += item.quantity
            
            results.append({
                "product_id": item.product_id,
                "type": target_type,
                "sku": sku,
                "action": payload.action,
                "quantity": item.quantity,
                "original_stock": original_stock,
                "new_stock": target_object.stock
            })
    
    # Log activity
    await log_activity(
        db,
        entity_type="inventory_batch",
        entity_id="batch",
        action=payload.action,
        details={
            "items_count": len(payload.items), 
            "action": payload.action,
            "reference_ids": [item.reference_id for item in payload.items if item.reference_id]
        },
        performed_by="system"
    )
    
    await db.commit()
    
    return {
        "message": f"Batch {payload.action} operation completed successfully",
        "results": results
    }


# ============================================================================
# OLD WAREHOUSE/PRODUCTSTOCK ENDPOINTS - COMMENTED OUT
# These endpoints used the multi-warehouse ProductStock system
# Keeping for reference if multi-warehouse support is needed in the future
# ============================================================================

# @router.post("/create-warehouse", status_code=status.HTTP_201_CREATED)
# async def create_warehouse(
#     warehouse: WarehouseCreate, db: AsyncSession = Depends(get_db)
# ):
#     # Check if warehouse already exists
#     existing = await db.execute(
#         select(Warehouse).where(
#             Warehouse.name == warehouse.name, Warehouse.location == warehouse.location
#         )
#     )
#     existing_warehouse = existing.scalar_one_or_none()
#     if existing_warehouse:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail=f"Warehouse '{warehouse.name} and {warehouse.location}' already exists.",
#         )
#
#     # Create warehouse
#     new_warehouse = Warehouse(name=warehouse.name, location=warehouse.location)
#     db.add(new_warehouse)
#     await db.flush()  # Get new_warehouse.id before adding stocks
#     await log_activity(
#         db,
#         entity_type="warehouse",
#         entity_id=new_warehouse.id,
#         action="create",
#         details={"name": new_warehouse.name, "location": new_warehouse.location},
#         performed_by="admin"
#     )
#
#     await db.commit()
#     await db.refresh(new_warehouse)
#
#     return {
#         "message": "Warehouse created successfully",
#         "warehouse": {
#             "id": new_warehouse.id,
#             "name": new_warehouse.name,
#             "location": new_warehouse.location,
#         },
#     }


# @router.post("/update-warehouse-details")
# async def update_stock(
#     stockdata: WarehouseBase,
#     db: AsyncSession = Depends(get_db),
# ):
#     # 1️⃣ Check if product exists
#     product_result = await db.execute(
#         select(Product).where(Product.id == stockdata.product_id)
#     )
#     product = product_result.scalar_one_or_none()
#     if not product:
#         raise HTTPException(status_code=404, detail="Product not found")
#
#     # 2️⃣ Check if warehouse exists
#     warehouse_result = await db.execute(
#         select(Warehouse).where(Warehouse.id == stockdata.warehouse_id)
#     )
#     warehouse = warehouse_result.scalar_one_or_none()
#     if not warehouse:
#         raise HTTPException(status_code=404, detail="Warehouse not found")
#
#     # 🔹 Optional: Update warehouse details if provided
#     if stockdata.name is not None:
#         warehouse.name = stockdata.name
#     if stockdata.location is not None:
#         warehouse.location = stockdata.location
#
#     # 3️⃣ Check if stock record exists
#     stock_result = await db.execute(
#         select(ProductStock).where(
#             ProductStock.product_id == stockdata.product_id,
#             ProductStock.warehouse_id == stockdata.warehouse_id,
#         )
#     )
#     stock = stock_result.scalar_one_or_none()
#
#     # 4️⃣ Update or create stock record
#     if stock:
#         stock.quantity = stockdata.quantity
#     else:
#         stock = ProductStock(
#             product_id=stockdata.product_id,
#             warehouse_id=stockdata.warehouse_id,
#             quantity=stockdata.quantity,
#         )
#         db.add(stock)
#
#     # 5️⃣ Commit all changes
#     await log_activity(
#         db,
#         entity_type="warehouse",
#         entity_id=warehouse.id,
#         action="update",
#         details={"name": warehouse.name, "location": warehouse.location, "stock_updated": True},
#         performed_by="admin"
#     )
#     await db.commit()
#     await db.refresh(stock)
#
#     return {
#         "message": "Stock updated successfully",
#         "data": {
#             "product_id": stockdata.product_id,
#             "warehouse_id": stockdata.warehouse_id,
#             "quantity": stockdata.quantity,
#             "warehouse_name": warehouse.name,
#             "warehouse_location": warehouse.location,
#         },
#     }


# @router.get("/list-warehouses")
# async def list_warehouses(
#     db: AsyncSession = Depends(get_db),
#     status: str | None = Query(
#         "active",
#         description="Filter warehouses by status: 'active', 'inactive', or 'all'",
#     ),
# ):
#
#     allowed_status = ["active", "inactive", "all"]
#     if status.lower() not in allowed_status:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail=f"Invalid status filter. Choose from {allowed_status}.",
#         )
#
#     # 👇 load products AND variants
#     query = select(Warehouse).options(
#         selectinload(Warehouse.stocks).selectinload(ProductStock.product),
#         selectinload(Warehouse.stocks).selectinload(ProductStock.variant),
#     )
#
#     if status.lower() != "all":
#         query = query.where(Warehouse.status == status.lower())
#
#     result = await db.execute(query)
#     warehouses = result.scalars().unique().all()
#
#     warehouse_data = []
#
#     for warehouse in warehouses:
#         stock_items = []
#         total_quantity = 0
#
#         for stock in warehouse.stocks:
#
#             product = stock.product
#             variant = stock.variant  # 👈 may be None
#
#             if not product:
#                 continue
#
#             stock_items.append(
#                 {
#                     "product_id": product.id,
#                     "product_name": product.title,
#                     "product_sku": product.sku,
#                     # 👇 show variant details only when present
#                     "variant_id": variant.id if variant else None,
#                     "variant_title": variant.title if variant else None,
#                     "variant_sku": variant.sku if variant else None,
#                     "quantity": stock.quantity,
#                 }
#             )
#
#             total_quantity += stock.quantity
#
#         warehouse_data.append(
#             {
#                 "id": warehouse.id,
#                 "name": warehouse.name,
#                 "location": warehouse.location,
#                 "status": warehouse.status,
#                 "total_quantity": total_quantity,
#                 "products": stock_items,
#             }
#         )
#
#     return {"warehouses": warehouse_data, "count": len(warehouse_data)}


# @router.put(
#     "/update-status/{warehouse_id}",
#     status_code=status.HTTP_200_OK,
# )
# async def update_warehouse_status(
#     warehouse_id: str,
#     data: WarehouseStatusUpdate,
#     db: AsyncSession = Depends(get_db),
# ):
#     # Validate status input
#     if data.status.lower() not in ["active", "inactive"]:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail="Invalid status. Allowed values are 'active' or 'inactive'.",
#         )
#
#     # Find the warehouse
#     result = await db.execute(select(Warehouse).where(Warehouse.id == warehouse_id))
#     warehouse = result.scalar_one_or_none()
#
#     if not warehouse:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND,
#             detail=f"Warehouse with ID '{warehouse_id}' not found.",
#         )
#
#     # Update status
#     warehouse.status = data.status.lower()
#     await db.commit()
#     await db.refresh(warehouse)
#
#     return {
#         "id": warehouse.id,
#         "name": warehouse.name,
#         "status": warehouse.status,
#         "message": f"Warehouse status updated to '{warehouse.status}'.",
#     }


# @router.delete(
#     "/delete-warehouse/{warehouse_id}",
#     status_code=status.HTTP_200_OK,
# )
# async def delete_warehouse(
#     warehouse_id: str,
#     db: AsyncSession = Depends(get_db),
# ):
#     # Find the warehouse
#     result = await db.execute(select(Warehouse).where(Warehouse.id == warehouse_id))
#     warehouse = result.scalar_one_or_none()
#
#     if not warehouse:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND,
#             detail="Warehouse not found.",
#         )
#
#     # Delete the warehouse
#     await db.delete(warehouse)
#     await db.commit()
#
#     return {"message": "Warehouse deleted successfully."}


# @router.post("/set-update-stock")
# async def set_product_stock(
#     body: SetStockRequest,
#     db: AsyncSession = Depends(get_db),
# ):
#     # ---- validate product exists ----
#     result = await db.execute(select(Product).where(Product.id == body.product_id))
#     product = result.scalar_one_or_none()
#
#     if not product:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND,
#             detail="Product not found.",
#         )
#     # ---- validate warehouse exists ----
#     await validate_warehouse(db, body.warehouse_name)
#     # ---- if variant provided, validate it belongs to the product ----
#     if body.variant_id:
#         vr = await db.execute(
#             select(ProductVariant).where(
#                 ProductVariant.id == body.variant_id,
#                 ProductVariant.product_id == body.product_id,
#             )
#         )
#         variant = vr.scalar_one_or_none()
#
#         if not variant:
#             raise HTTPException(
#                 status_code=400,
#                 detail="Variant does not belong to this product.",
#             )
#
#     # ---- do the stock update ----
#     stock = await set_stock(
#         db,
#         product_id=body.product_id,
#         warehouse_name=body.warehouse_name,
#         quantity=body.quantity,
#         variant_id=body.variant_id,
#         location=body.location,
#         added_by="system",
#     )
#
#     await db.commit()
#
#     return {
#         "message": "Stock updated successfully.",
#         "product_id": body.product_id,
#         "variant_id": body.variant_id,
#         "warehouse_name": body.warehouse_name,
#         "quantity": stock.quantity,
#     }

