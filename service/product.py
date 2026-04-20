from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import APIRouter, Depends, Query, status, HTTPException
from sqlalchemy import select, and_, or_, func
from sqlalchemy.orm import selectinload

from models.product import Product, ProductVariant, ProductStats
from models.brand import Brand
from models.category import Category
from models.warehouse import ProductStock
from models.return_policy import ReturnPolicy
from models.promotions import Promotion
from models.product_highlights import ProductHighlight, ProductHighlightItem

from schemas.product import (
    ProductFilter,
    ProductResponse,
    PaginatedProductResponse,
    ProductArchiveRequest,
    ProductListRequest,
    ProductResponseWithImages,
    ProductUpdateWithImages,
    ProductCreateWithVariantImages,
    ProductResponseWithVariantImages,
)

from deps import get_db
from repositories.product.product_repository import ProductRepository
from apis.v1.utils import generate_unique_product_code
from apis.v1.import_export import make_slug
from service.warehouse import validate_warehouse, set_stock
from utils.activity_logger import log_activity
from utils.admin_auth import require_catalog_supervisor

router = APIRouter()

async def record_product_activity(
    db: AsyncSession, product_id: str, view=False, order=False, cart=False
):
    repo = ProductRepository(db)
    await repo.record_activity(product_id, view, order, cart)
    # Note: Committing inside activity record is usually risky if called from other operations.
    # The repository version uses flush() allowing the calling service to commit.
    await db.commit() 


def normalize_text(text: str) -> str:
    """Remove spaces and special characters for better fuzzy matching."""
    import re
    return re.sub(r"[^a-z0-9]", "", text.lower())


async def get_fuzzy_matched_ids(db: AsyncSession, model, field, search_value: str):
    repo = ProductRepository(db)
    return await repo.get_fuzzy_matched_ids(model, field, search_value)


async def resolve_return_policy(db: AsyncSession, product_id: str, country: str | None):
    repo = ProductRepository(db)
    return await repo.resolve_return_policy(product_id, country)


async def build_filters(db: AsyncSession, filters: ProductFilter, is_searching: bool, user_id: str):
    repo = ProductRepository(db)
    return await repo.build_filters(filters, is_searching)


async def build_price_filter(
    db: AsyncSession,
    ids_subq,
    filters: list,
):
    # This remains in service as it's a UI-focused filter builder, 
    # but still uses DB calls. We can keep it or move it to a UI service.
    from models.product import Product
    price_row = await db.execute(
        select(func.min(Product.price), func.max(Product.price)).where(
            Product.id.in_(ids_subq), Product.price.isnot(None)
        )
    )

    min_price, max_price = price_row.one_or_none() or (None, None)
    if min_price is None or max_price is None:
        return

    price_ranges = [
        (0, 50, "Under 50"),
        (50, 100, "50 to 100"),
        (100, 200, "100 to 200"),
        (200, 9999999, "200 and Above"),
    ]

    price_steps = []
    for lo, hi, label in price_ranges:
        count = await db.scalar(
            select(func.count(Product.id)).where(
                Product.id.in_(ids_subq),
                Product.price >= lo,
                Product.price < hi if hi < 9999999 else Product.price >= lo,
            )
        )
        if count > 0:
            price_steps.append(f"{label} ({count})")

    filters.append({"attribute": "Price", "values": price_steps})


def apply_active_constraints(query):
    # Enforces active constraints. Safe to use in service or repo.
    from models.brand import Brand
    from models.category import Category
    from models.product import Product
    return query.where(Product.status == "active") \
                .join(Brand, Brand.id == Product.brand_id) \
                .where(Brand.is_active.is_(True)) \
                .join(Category, Category.id == Product.category_id) \
                .where(Category.is_active.is_(True))


async def build_product_query(db: AsyncSession, filters: ProductFilter, user: Any):
    repo = ProductRepository(db)
    query = await repo.build_product_query(filters)
    
    # Subquery for counting
    count_query = select(func.count()).select_from(query.subquery())
    return query, count_query


async def get_reserved_stock_map(db: AsyncSession, ids: list[str]) -> dict:
    repo = ProductRepository(db)
    return await repo.get_reserved_stock_map(ids)

async def get_products(db: AsyncSession, filters: ProductFilter, user: dict):
    query, count_query = await build_product_query(db, filters, user)

    total = await db.scalar(count_query)

    query = query.offset((filters.page - 1) * filters.limit).limit(filters.limit)
    vendor = user.get("vendor")
    if user.get("role") != "Superadmin":
        if vendor:
            query = query.where(Product.vendor_id == vendor.get("id"))
    else:
        if vendor:
            query = query.where(or_(Product.vendor_id == None, Product.vendor_id == vendor.get("id")))

    result = await db.execute(query)
    products = result.scalars().unique().all()

    ids = [p.id for p in products]
    for p in products:
        ids.extend([v.id for v in p.variants])
        
    reserved_map = await get_reserved_stock_map(db, ids)
    data = serialize_products(products, reserved_map)

    return {
        "page": filters.page,
        "limit": filters.limit,
        "total": total,
        "pages": (total + filters.limit - 1) // filters.limit,
        "data": data
    }

def serialize_products(products, reserved_map=None):
    if reserved_map is None:
        reserved_map = {}
    data = []

    for product in products:
        eff_stock = max(0, product.stock - reserved_map.get(product.id, 0))
        
        variants_data = []
        for v in product.variants:
            variants_data.append({
                "id": v.id,
                "sku": v.sku,
                "title": v.title,
                "price": v.price,
                "cost_price": v.cost_price,
                "stock": max(0, v.stock - reserved_map.get(v.id, 0)),
                "length": v.length,
                "width": v.width,
                "height": v.height,
                "weight": v.weight,
                "ships_from_location": v.ships_from_location,
                "handling_time_days": v.handling_time_days,
                "estimated_shipping_cost": v.estimated_shipping_cost,
                "product_margin_percent": v.product_margin_percent,
                "product_margin_amount": v.product_margin_amount,
                "profit": v.profit,
                "rrp_price": v.rrp_price,
                "attributes": v.attributes,
                "images": v.images,
            })

        data.append({
            "id": product.id,
            "product_id": product.id,
            "unique_code": product.unique_code,
            "vendor_id": product.vendor_id,
            "sku": product.sku,
            "title": product.title,
            "description": product.description,
            "slug": product.slug,
            "price": product.price,
            "cost_price": product.cost_price,
            "rrp_price": product.rrp_price,
            "stock": eff_stock,
            "status": product.status,
            "product_condition": product.product_condition,
            "weight": product.weight,
            "length": product.length,
            "width": product.width,
            "height": product.height,
            "ean": product.ean,
            "asin": product.asin,
            "mpn": product.mpn,
            "free_shipping": product.free_shipping,
            "fast_dispatch": product.fast_dispatch,
            "ships_from_location": product.ships_from_location,
            "handling_time_days": product.handling_time_days,
            "brand_name": product.brand.name if product.brand else None,
            "category_name": product.category.name if product.category else None,
            "brand_id": product.brand_id,
            "category_id": product.category_id,
            "images": product.images,
            "variants": variants_data,
            "is_variant": False
        })

    return data


async def get_variant(db: AsyncSession, product_id: str):
    query = (
        select(Product, ProductVariant)
        .join(ProductVariant)
        .options(
            selectinload(Product.brand),
            selectinload(Product.category),
            selectinload(Product.images),
            selectinload(ProductVariant.images),
            selectinload(ProductVariant.attributes),
        )
        .where(
            or_(
                ProductVariant.id == product_id,
                ProductVariant.sku == product_id,
            )
        )
    )

    result = await db.execute(query)
    return result.first()

def serialize_product(product, reserved_map=None):
    if reserved_map is None:
        reserved_map = {}
        
    eff_stock = max(0, product.stock - reserved_map.get(product.id, 0))
    
    variants_data = []
    for v in product.variants:
        variants_data.append({
            "id": v.id,
            "sku": v.sku,
            "title": v.title,
            "price": v.price,
            "cost_price": v.cost_price,
            "stock": max(0, v.stock - reserved_map.get(v.id, 0)),
            "length": v.length,
            "width": v.width,
            "height": v.height,
            "weight": v.weight,
            "ships_from_location": v.ships_from_location,
            "handling_time_days": v.handling_time_days,
            "estimated_shipping_cost": v.estimated_shipping_cost,
            "product_margin_percent": v.product_margin_percent,
            "product_margin_amount": v.product_margin_amount,
            "profit": v.profit,
            "rrp_price": v.rrp_price,
            "attributes": v.attributes,
            "images": v.images,
        })

    return {
        "id": product.id,
        "product_id": product.id,
        "unique_code": product.unique_code,
        "vendor_id": product.vendor_id,
        "sku": product.sku,
        "title": product.title,
        "description": product.description,
        "slug": product.slug,
        "price": product.price,
        "cost_price": product.cost_price,
        "rrp_price": product.rrp_price,
        "stock": eff_stock,
        "status": product.status,
        "product_condition": product.product_condition,
        "weight": product.weight,
        "length": product.length,
        "width": product.width,
        "height": product.height,
        "ean": product.ean,
        "asin": product.asin,
        "mpn": product.mpn,
        "free_shipping": product.free_shipping,
        "fast_dispatch": product.fast_dispatch,
        "ships_from_location": product.ships_from_location,
        "handling_time_days": product.handling_time_days,
        "brand_name": product.brand.name if product.brand else None,
        "category_name": product.category.name if product.category else None,
        "brand_id": product.brand_id,
        "category_id": product.category_id,
        "variants": variants_data,
        "images": product.images,
        "is_variant": False,
    }


def serialize_variant_product(product, variant, reserved_map=None):
    if reserved_map is None:
        reserved_map = {}
        
    eff_stock = max(0, variant.stock - reserved_map.get(variant.id, 0))

    return {
        "id": variant.id,
        "product_id": product.id,
        "unique_code": product.unique_code,
        "vendor_id": product.vendor_id,
        "sku": variant.sku,
        "title": variant.title,
        "description": variant.description or product.description,
        "slug": variant.slug or product.slug,
        "price": variant.price,
        "cost_price": variant.cost_price if variant.cost_price is not None else product.cost_price,
        "rrp_price": variant.rrp_price if variant.rrp_price is not None else product.rrp_price,
        "stock": eff_stock,
        "status": variant.status,
        "product_condition": variant.product_condition or product.product_condition,
        "weight": variant.weight or product.weight,
        "length": variant.length or product.length,
        "width": variant.width or product.width,
        "height": variant.height or product.height,
        "ean": variant.ean or product.ean,
        "asin": variant.asin or product.asin,
        "mpn": variant.mpn or product.mpn,
        "free_shipping": variant.free_shipping if variant.free_shipping is not None else product.free_shipping,
        "fast_dispatch": variant.fast_dispatch if variant.fast_dispatch is not None else product.fast_dispatch,
        "ships_from_location": variant.ships_from_location or product.ships_from_location,
        "handling_time_days": variant.handling_time_days if variant.handling_time_days is not None else product.handling_time_days,
        "brand_name": product.brand.name if product.brand else None,
        "category_name": product.category.name if product.category else None,
        "brand_id": product.brand_id,
        "category_id": product.category_id,
        "images": variant.images if variant.images else product.images,
        "variants": [],
        "is_variant": True,
    }
async def get_parent_product(db: AsyncSession, product_id: str):
    query = (
        select(Product)
        .options(
            selectinload(Product.variants).selectinload(ProductVariant.attributes),
            selectinload(Product.variants).selectinload(ProductVariant.images),
            selectinload(Product.brand),
            selectinload(Product.images),
            selectinload(Product.category),
            selectinload(Product.seo),
            selectinload(Product.warehouse_stocks).selectinload(ProductStock.warehouse),
        )
        .where(
            or_(
                Product.id == product_id,
                Product.sku == product_id,
            )
        )
    )

    result = await db.execute(query)
    return result.scalar_one_or_none()
async def get_product(db: AsyncSession, product_id: str):
    product_id = product_id.strip()

    variant = await get_variant(db, product_id)

    if variant:
        product_obj, variant_obj = variant
        ids = [product_obj.id, variant_obj.id]
        reserved_map = await get_reserved_stock_map(db, ids)
        return serialize_variant_product(product_obj, variant_obj, reserved_map)

    product = await get_parent_product(db, product_id)

    if not product:
        raise HTTPException(404, "Product or variant not found")

    ids = [product.id]
    for v in product.variants:
        ids.append(v.id)
    reserved_map = await get_reserved_stock_map(db, ids)

    return serialize_product(product, reserved_map)



async def __get_product_variant(db: AsyncSession, product_id: str):
    query = select(ProductVariant).where(ProductVariant.id == product_id)
    result = await db.execute(query)
    return result.scalar_one_or_none()
async def _get_active_product(db: AsyncSession, product_id: str):
    status_query = select(Product.id, Product.status).where(
        or_(
            Product.id == product_id,
            Product.unique_code == product_id,
            Product.slug == product_id
        )
    )
    status_result = await db.execute(status_query)
    product_row = status_result.first()
    if not product_row:
         raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Product not found"
        )
    return product_row
