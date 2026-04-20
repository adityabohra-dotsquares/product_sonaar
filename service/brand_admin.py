from sqlalchemy import select, func, case
from sqlalchemy.exc import IntegrityError
from models.brand import Brand
from models.product import Product
from fastapi import HTTPException, status
from fastapi.responses import StreamingResponse
import io
import csv
import json
import utils.constants as constants
from apis.v1.import_export import make_slug
from utils.activity_logger import log_activity
from service.redis import get_redis_url
import redis.asyncio as redis

BRAND_CACHE_TTL = 300  # 5 minutes


async def _invalidate_brand_cache() -> None:
    """Clear all brand list cache keys."""
    try:
        r = redis.from_url(get_redis_url())
        async with r:
            keys = await r.keys("brand:list:*")
            if keys:
                await r.delete(*keys)
    except Exception:
        pass  # Cache invalidation failures must never break the write path


async def create_brand(db, brand):
    # Check if brand name already exists
    try:
        result = await db.execute(select(Brand).where(Brand.name == brand.name))
        existing_brand = result.scalars().first()
        if existing_brand:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=constants.messages.get("brand_name_exists"),
            )

        new_brand = Brand(
            name=brand.name,
            slug=make_slug(brand.name),
            image_url=brand.image_url,
            logo_url=str(brand.logo_url) if brand.logo_url else None,
        )
        db.add(new_brand)
        await db.flush()
        await log_activity(
            db,
            entity_type="brand",
            entity_id=new_brand.id,
            action="create",
            details={"name": new_brand.name},
            performed_by="admin",
        )
        await db.commit()
        await db.refresh(new_brand)
        await _invalidate_brand_cache()
        return new_brand
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=constants.messages.get("brand_name_exists"),
        )
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        )


async def list_brands(
    db, page, size, status, q, sort_by, sort_dir, only_with_products, download
):
    # --- Redis cache check ---
    cache_key = f"brand:list:{page}:{size}:{status}:{q}:{sort_by}:{sort_dir}:{only_with_products}"
    try:
        r = redis.from_url(get_redis_url())
        async with r:
            cached = await r.get(cache_key)
            if cached:
                data = json.loads(cached)
                return data["total"], data["rows"]
    except Exception:
        pass  # Cache miss — fall through to DB

    offset = (page - 1) * size
    # Filters
    filters = []
    if status == "active":
        filters.append(Brand.is_active == True)
    elif status == "inactive":
        filters.append(Brand.is_active == False)
    if q:
        filters.append(Brand.name.ilike(f"%{q.strip()}%"))

    # Sorting
    sort_col = Brand.name if sort_by == "name" else Brand.created_at
    order_clause = sort_col.asc() if sort_dir == "asc" else sort_col.desc()

    # Query
    query = (
        select(
            Brand.id,
            Brand.name,
            Brand.slug,
            Brand.logo_url,
            Brand.image_url,
            Brand.is_active,
            func.count(Product.id).label("total_products"),
            func.count(case((Product.status == "active", 1))).label("active_products"),
            func.count(case((Product.status != "active", 1))).label(
                "inactive_products"
            ),
        )
        .outerjoin(Product, Product.brand_id == Brand.id)
        .where(*filters)
        .group_by(Brand.id)
    )

    if only_with_products:
        query = query.having(func.count(Product.id) > 0)

    query = query.order_by(order_clause)

    # Total count
    total_query = select(func.count(Brand.id)).where(*filters)
    if only_with_products:
        total_query = total_query.where(
            select(Product.id).where(Product.brand_id == Brand.id).exists()
        )
    total = await db.scalar(total_query)

    # Pagination
    result = await db.execute(query.offset(offset).limit(size))
    rows = [dict(r) for r in result.mappings().all()]

    # --- Store in Redis ---
    try:
        r = redis.from_url(get_redis_url())
        async with r:
            await r.setex(
                cache_key, BRAND_CACHE_TTL, json.dumps({"total": total, "rows": rows})
            )
    except Exception:
        pass

    return total, rows

    # CSV download
    # if download == "yes":
    #     output = io.StringIO()
    #     writer = csv.writer(output)

    #     writer.writerow(
    #         [
    #             "Brand ID",
    #             "Brand Name",
    #             "Total Products",
    #             "Active Products",
    #             "Inactive Products",
    #         ]
    #     )

    #     for r in rows:
    #         writer.writerow(
    #             [
    #                 r["id"],
    #                 r["name"],
    #                 r["total_products"],
    #                 r["active_products"],
    #                 r["inactive_products"],
    #             ]
    #         )

    #     output.seek(0)
    #     return StreamingResponse(
    #         output,
    #         media_type="text/csv",
    #         headers={"Content-Disposition": "attachment; filename=brand_report.csv"},
    #     )


async def _get_brand(db, brand_id):
    result = await db.execute(select(Brand).where(Brand.id == brand_id))
    brand = result.scalar_one_or_none()
    if not brand:
        raise HTTPException(
            status_code=404,
            detail=constants.messages.get("brand_not_found", "Brand not found"),
        )
    return brand


async def _update_brand(db, brand_id, brand_data):
    result = await db.execute(select(Brand).where(Brand.id == brand_id))
    brand = result.scalar_one_or_none()
    if not brand:
        raise HTTPException(
            status_code=404,
            detail=constants.messages.get("brand_not_found", "Brand not found"),
        )

    if brand_data.name and brand_data.name != brand.name:
        existing_result = await db.execute(
            select(Brand).where(Brand.name == brand_data.name)
        )
        if existing_result.scalars().first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Brand with name '{brand_data.name}' already exists",
            )
        brand.slug = make_slug(brand_data.name)

    for field, value in brand_data.dict(exclude_unset=True).items():
        if field == "name" and value != brand.name:
            brand.slug = make_slug(value)
        setattr(brand, field, value)

    await log_activity(
        db,
        entity_type="brand",
        entity_id=brand.id,
        action="update",
        details={"name": brand.name},
        performed_by="admin",
    )
    await db.commit()
    await db.refresh(brand)
    await _invalidate_brand_cache()
    return brand


async def _delete_brand(db, brand_id):
    result = await db.execute(select(Brand).where(Brand.id == brand_id))
    brand = result.scalar_one_or_none()
    if not brand:
        raise HTTPException(
            status_code=404, detail=constants.messages.get("brand_not_found")
        )

    # Check if brand has products
    product_count = await db.scalar(
        select(func.count(Product.id)).where(Product.brand_id == brand_id)
    )
    if product_count > 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=constants.messages.get(
                "brand_cannot_delete_with_products",
                "Cannot delete brand with {product_count} associated products. Delete the products first.",
            ).format(product_count=product_count),
        )

    await db.delete(brand)
    await log_activity(
        db,
        entity_type="brand",
        entity_id=brand_id,
        action="delete",
        details={"name": brand.name},
        performed_by="admin",
    )
    await db.commit()
    await _invalidate_brand_cache()
