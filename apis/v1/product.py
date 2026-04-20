from collections import defaultdict
from datetime import datetime, timedelta
import json
import hashlib
import time
import logging

logger = logging.getLogger(__name__)
from models.review import Review, ReviewStats
from service.product import normalize_text
from service.redis import get_redis_url
import redis.asyncio as redis
from fastapi import Request
from fastapi import APIRouter, status, Query, UploadFile, File, Form
from fastapi import Depends, HTTPException
from models.brand import Brand
from rapidfuzz import fuzz
from typing import List, Optional, Annotated, Set
from sqlalchemy import text, bindparam, or_, and_, false
from fastapi.responses import JSONResponse, Response
from fastapi.encoders import jsonable_encoder
from service.product import *
from decimal import Decimal, InvalidOperation
from service.product import resolve_return_policy
import os, re
import database, uuid
from schemas.product import ProductStatusOut, ProductStatusUpdate
from models.category import Category, CategoryAttribute
from service.category import get_all_subcategory_ids_cte, get_active_category_ids_cte, get_active_subtree_ids, get_all_categories_flat, build_category_tree_fast
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert, select, delete, DECIMAL
from sqlalchemy.orm import selectinload, contains_eager
from models.product import Product, ProductImage, ProductStats, RecentlyViewed
from models.promotions import Promotion
from schemas.product import (
    ProductResponse,
    PaginatedProductResponse,
    ProductArchiveRequest,
    ProductListRequest,
    ProductResponseWithImages,
    ProductUpdateWithImages,
    ProductCreateWithVariantImages,
    ProductResponseWithVariantImages,
)
from service.warehouse import validate_warehouse, set_stock
from models.warehouse import Warehouse, ProductStock
from deps import get_db
from models.product import Product, Attribute, ProductVariant, ProductVariantImage
from sqlalchemy import func
from utils.utils import create_search_history
from utils.promotions_utils import calculate_discounted_price, get_applicable_promotion
from utils.promotions_client import fetch_applicable_promotions, calculate_best_promotion
import re
from apis.v1.import_export import make_slug
from utils.gcp_bucket import upload_to_gcs
from utils.image_handler import download_and_upload_image
from service.product import build_price_filter
from urllib.parse import unquote, unquote_plus
from PIL import Image
from io import BytesIO
from io import BytesIO
from utils.product import build_bundle_products
from models.seo import ProductSEO
from models.product_highlights import ProductHighlight, ProductHighlightItem
from models.stock_reservation import StockReservation
from apis.v1.utils import generate_unique_product_code
from utils.activity_logger import log_activity
from utils.constants import messages
router = APIRouter()

def price_sort_key(value: str):
    # Extract the first number (e.g., "1,400" from "1,400-1,900 (0)")
    match = re.search(r"\d[\d,]*", value)
    if not match:
        return float("inf")

    # Remove commas and convert to int
    num = int(match.group(0).replace(",", ""))
    return num


def natural_sort_key(value: str):
    return [
        int(num) if num.isdigit() else num.lower() for num in re.split(r"(\d+)", value)
    ]


# ---------------- CREATE PRODUCT ----------------


@router.post(
    "/create-product",
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {"description": "Validation error (SKU/EAN exists, invalid product code, etc.)"},
    },
)
async def create_product(
    product: ProductCreateWithVariantImages,
    db: Annotated[AsyncSession, Depends(get_db)],
    user: Annotated[dict, Depends(require_catalog_supervisor)],
):  
    print("USER",user)
    # check product SKU
    existing_product = await db.execute(
        select(Product).where(Product.sku == product.sku)
    )
    if existing_product.scalar_one_or_none():
        print("PRODUCT SKU ALREADY EXISTS",product.sku)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=messages.get("product_sku_already_exist").format(product_sku=product.sku),
        )
    if product.product_code:
        code_len = len(product.product_code)
        if code_len < 12 or code_len > 14:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Product code must be between 12 and 14 characters long.",
            )

    # Check Product EAN uniqueness
    if product.ean and product.ean.strip():
        # Check against Product table
        existing_product_ean = await db.execute(
            select(Product).where(Product.ean == product.ean)
        )
        if existing_product_ean.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=messages.get("product_ean_already_exist").format(product_ean=product.ean),
            )
        
        # Check against ProductVariant table
        existing_variant_ean_check = await db.execute(
            select(ProductVariant).where(ProductVariant.ean == product.ean)
        )
        if existing_variant_ean_check.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=messages.get("product_ean_already_exist").format(product_ean=product.ean),
            )

    # Check Variant EAN uniqueness
    for variant in product.variants:
        if variant.ean and variant.ean.strip():
             # Check against Product table
            existing_product_ean_v = await db.execute(
                select(Product).where(Product.ean == variant.ean)
            )
            if existing_product_ean_v.scalar_one_or_none():
                 raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Product with EAN '{variant.ean}' already exists.",
                )

            # Check against ProductVariant table
            existing_variant_ean_v = await db.execute(
                select(ProductVariant).where(ProductVariant.ean == variant.ean)
            )
            if existing_variant_ean_v.scalar_one_or_none():
                 raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"ProductVariant with EAN '{variant.ean}' already exists.",
                )
    # check variant SKUs
    for variant in product.variants:
        existing_variant = await db.execute(
            select(ProductVariant).where(ProductVariant.sku == variant.sku)
        )
        if existing_variant.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Variant SKU '{variant.sku}' already exists.",
            )
    # validate warehouse
    if product.ships_from_location:
        await validate_warehouse(db, product.ships_from_location)
    # create product
    unique_code = await generate_unique_product_code(db)
    new_product = Product(
        unique_code=unique_code,
        sku=product.sku,
        title=product.title,
        description=product.description,
        slug=make_slug(product.title),
        tags=product.tags,
        price=product.price,
        cost_price=product.cost_price,
        status=product.status,
        weight=product.weight,
        unit="kg",
        supplier=product.supplier,
        country_of_origin=product.country_of_origin,
        product_id_type=product.product_id_type,
        ean=product.ean,
        asin=product.asin,
        mpn=product.mpn,
        category_id=product.category_id,
        brand_id=product.brand_id,
        stock=product.stock,
        key_features=product.key_features,
        is_battery_required=product.is_battery_required,
        precautionary_note=product.precautionary_note,
        care_instructions=product.care_instructions,
        warranty=product.warranty,
        rrp_price=product.rrp_price,
        ships_from_location=product.ships_from_location,
        handling_time_days=product.handling_time_days,
        estimated_shipping_cost=product.estimated_shipping_cost,
        product_margin_percent=product.product_margin_percent,
        product_margin_amount=product.product_margin_amount,
        profit=product.profit,
        length=product.length,
        width=product.width,
        height=product.height,
        product_code=product.product_code,
        # ean=product.product_code,
        added_by=user.get("admin_id"),
        vendor_id=user.get("vendor").get("id") if user.get("vendor") else None,
        product_condition=product.product_condition,
        fast_dispatch=product.fast_dispatch,
        free_shipping=product.free_shipping,
        hs_code=product.hs_code,
        bundle_group_code=product.bundle_group_code,
    )
    db.add(new_product)
    await db.flush()  # populates new_product.id
    await set_stock(
        db,
        product_id=new_product.id,
        warehouse_name=product.ships_from_location,
        quantity=product.stock,
        variant_id=None,
        added_by="system",
    )

    # create variants + attributes + variant images
    for variant_data in product.variants:
        if variant_data.ships_from_location:
            await validate_warehouse(db, variant_data.ships_from_location)
        new_variant = ProductVariant(
            product_id=new_product.id,
            title=variant_data.title,
            sku=variant_data.sku,
            price=variant_data.price,
            stock=variant_data.stock,
            cost_price=variant_data.cost_price,
            rrp_price=variant_data.rrp_price,
            ships_from_location=variant_data.ships_from_location
            or product.ships_from_location,
            handling_time_days=variant_data.handling_time_days,
            estimated_shipping_cost=variant_data.estimated_shipping_cost,
            product_margin_percent=variant_data.product_margin_percent,
            product_margin_amount=variant_data.product_margin_amount,
            profit=variant_data.profit,
            length=variant_data.length,
            width=variant_data.width,
            height=variant_data.height,
            hs_code=variant_data.hs_code,
            ean=variant_data.ean,
            bundle_group_code=variant_data.bundle_group_code,
            weight=variant_data.weight,
            # add other fields if needed
        )
        db.add(new_variant)
        await db.flush()  # get new_variant.id
        await set_stock(
            db,
            product_id=new_product.id,
            warehouse_name=variant_data.ships_from_location
            or product.ships_from_location,
            quantity=variant_data.stock,
            variant_id=new_variant.id,
            added_by="system",
        )

        # attributes (existing)
        for attr in getattr(variant_data, "attributes", []):
            db.add(
                Attribute(
                    name=attr.name,
                    value=attr.value,
                    variant_id=new_variant.id,
                )
            )

        # --- NEW: variant-level images ---
        # expect variant_data.images to be an iterable of { image_url, is_main? }
        for vimg in getattr(variant_data, "images", []):
            img_url = await download_and_upload_image(vimg.image_url, identifier=new_variant.sku)
            db.add(
                ProductVariantImage(
                    variant_id=new_variant.id,
                    image_url=img_url,
                    is_main=getattr(vimg, "is_main", False),
                    image_order=getattr(vimg, "image_order", None),
                    video_url=getattr(vimg, "video_url", None),
                )
            )

    # product-level images (existing)
    for img in getattr(product, "images", []):
        img_url = await download_and_upload_image(img.image_url, identifier=new_product.sku)
        db.add(
            ProductImage(
                product_id=new_product.id,
                image_url=img_url,
                is_main=getattr(img, "is_main", False),
                image_order=getattr(img, "image_order", None),
                video_url=getattr(img, "video_url", None),
            )
        )

    # product seo
    if product.seo:
        db.add(
            ProductSEO(
                product_id=new_product.id,
                page_title=product.seo.page_title,
                meta_description=product.seo.meta_description,
                meta_keywords=product.seo.meta_keywords,
                url_handle=product.seo.url_handle,
                canonical_url=product.seo.canonical_url,
            )
        )

    # Activity Log
    await log_activity(
        db,
        entity_type="product",
        entity_id=new_product.id,
        action="create",
        details={"sku": new_product.sku, "title": new_product.title},
        performed_by=user.get("admin_id"),
    )

    await db.commit()

    # reload product with variants, attributes, and both product & variant images
    result = await db.execute(
        select(Product)
        .options(
            # load variants, and for each variant load attributes and variant images
            selectinload(Product.variants).selectinload(ProductVariant.attributes),
            selectinload(Product.variants).selectinload(ProductVariant.images),
            selectinload(Product.brand),
            selectinload(Product.category),
            # load product-level images
            selectinload(Product.images),
            selectinload(Product.seo),
        )
        .where(Product.id == new_product.id)
    )

    product_with_variants = result.scalar_one()
    # product_with_variants.brand_name = product_with_variants.brand.name
    # product_with_variants.category_name = product_with_variants.category.name
    return product_with_variants

# ---------------- LIST PRODUCTS HELPER ----------------
async def _get_products_list(
    request: Request,
    db: AsyncSession,
    sku: str | None = None,
    name: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    category_id: str | None = None,
    category_slug: str | None = None,
    brand_id: str | None = None,
    brand_slug: str | None = None,
    sort_by: str = "newly_added",
    page: int = 1,
    limit: int = 10,
    free_shipping: bool | None = None,
    promotions_only: bool = False,
    fast_dispatch: bool | None = None,
    product_condition: str | None = None,
    todays_special: bool | None = None,
    sales: bool | None = None,
    clearance: bool | None = None,
    limited_time_deal: bool | None = None,
    new_arrivals: bool | None = None,
    vendor_id: str | None = None,
):
    # Initialize redis
    redis_client = redis.from_url(get_redis_url())
    logger.info(f"promotions_only: {promotions_only}")
    # Create a unique cache key based on all parameters
    params = {
        "sku": sku,
        "name": name,
        "min_price": min_price,
        "max_price": max_price,
        "category_id": category_id,
        "category_slug": category_slug,
        "brand_id": brand_id,
        "brand_slug": brand_slug,
        "sort_by": sort_by,
        "page": page,
        "limit": limit,
        "free_shipping": free_shipping,
        "fast_dispatch": fast_dispatch,
        "product_condition": product_condition,
        "promotions_only": promotions_only,
        "vendor_id": vendor_id,
        "todays_special": todays_special,
        "sales": sales,
        "clearance": clearance,
        "limited_time_deal": limited_time_deal,
        "new_arrivals": new_arrivals,
        "custom_filters": str(request.query_params)
    }
    
    # Sort params to ensure consistent key
    param_string = json.dumps(params, sort_keys=True, default=str)
    # v3: Nested variants + Contextual filters fix
    cache_key = f"products_list_v4:{hashlib.md5(param_string.encode()).hexdigest()}"

    # Try to get from cache
    # try:
    #     cached_data = await redis_client.get(cache_key)
    #     # logger.info(f"cached_data {cached_data}")
    #     if cached_data:
    #         return json.loads(cached_data)
    # except Exception as e:
    #     logger.error(f"Redis get error: {e}")

    import time
    start_time = time.time()
    
    query = select(Product).options(
        selectinload(Product.variants).selectinload(ProductVariant.attributes),
        selectinload(Product.review_stats),
        selectinload(Product.images),
        selectinload(Product.variants).selectinload(ProductVariant.images),
        contains_eager(Product.brand),
        contains_eager(Product.category),
        selectinload(Product.highlight_items).selectinload(ProductHighlightItem.highlight),
        selectinload(Product.stats),
    )
    # query = query.where(Product.status == "active")
    query = apply_active_constraints(query)
    logger.info(f"query building {time.time() - start_time}")
    
    # --- Promotions Filter ---
    promotions = await fetch_applicable_promotions()
    logger.info(f"promotions {promotions}")

    
    if promotions_only:
        promo_product_ids = set()
        promo_category_ids = set()
        promo_brand_ids = set()
        
        for promo in promotions:
            scope = promo.get("scope")
            ref_id = promo.get("reference_id")
            if scope == "product":
                if ref_id:
                    promo_product_ids.add(ref_id)
                product_identifiers = promo.get("product_identifiers") or []
                for p_id in product_identifiers:
                    if p_id:
                        promo_product_ids.add(p_id)
            elif scope == "category":
                if ref_id:
                    promo_category_ids.add(ref_id)
            elif scope == "brand":
                if ref_id:
                    promo_brand_ids.add(ref_id)
        print("----promo_product_ids", promo_product_ids)
        print("----promo_category_ids", promo_category_ids)
        print("----promo_brand_ids", promo_brand_ids)
        promo_conditions = []
        if promo_product_ids:
            promo_conditions.append(Product.id.in_(list(promo_product_ids)))
        if promo_category_ids:
            all_promo_cat_ids = set()
            for cid in promo_category_ids:
                subtree = await get_active_subtree_ids(db, cid)
                all_promo_cat_ids.update(subtree)
            if all_promo_cat_ids:
                promo_conditions.append(Product.category_id.in_(list(all_promo_cat_ids)))
        if promo_brand_ids:
            promo_conditions.append(Product.brand_id.in_(list(promo_brand_ids)))
        
        if promo_conditions:
            query = query.where(or_(*promo_conditions))
        else:
            # No promotions found, so no products should match
            query = query.where(false())

    # --- Filters ---
    # Note: Filters (Category, Brand, Search) apply to the Parent Product.
    # If a Parent Product matches, ALL its variants are returned in the response (flattened).
    if sku:
        query = query.where(Product.sku == sku)

    if min_price is not None:
        query = query.where(Product.price >= min_price)
    if max_price is not None:
        query = query.where(Product.price <= max_price)
    
    if free_shipping is not None:
        query = query.where(Product.free_shipping == free_shipping)
    if fast_dispatch is not None:
        query = query.where(Product.fast_dispatch == fast_dispatch)
    if product_condition:
        query = query.where(Product.product_condition == product_condition)

    # --- Special Offers Filters ---
    if todays_special:
        query = query.where(Product.highlight_items.any(
            ProductHighlightItem.highlight.has(ProductHighlight.type.in_(["Today's Deal", "Todays Deals"]))
        ))

    if sales:
        # We can use the already fetched promotions list to filter
        promo_p_ids = {p.get("reference_id") for p in promotions if p.get("scope") == "product"}
        # Also need to check category/brand promos if we want to be thorough, 
        # but for simplicity and matching tag logic (which uses calculate_best_promotion),
        # we can just check if any promo applies.
        # Actually, a better way is a subquery.
        from models.promotions import Promotion
        now = datetime.now()
        query = query.where(Product.id.in_(
            select(Promotion.reference_id).where(
                Promotion.offer_type == 'product',
                Promotion.status == 'active',
                Promotion.start_date <= now,
                Promotion.end_date >= now
            )
        ))

    if clearance:
        query = query.where(Product.highlight_items.any(
            ProductHighlightItem.highlight.has(ProductHighlight.type == "Clearance")
        ))

    if limited_time_deal:
        query = query.where(Product.highlight_items.any(
            ProductHighlightItem.highlight.has(ProductHighlight.type.in_(["Hot Deals", "Trending Deals"]))
        ))

    if new_arrivals:
        fifteen_days_ago = datetime.now() - timedelta(days=15)
        query = query.where(or_(
            Product.created_at >= fifteen_days_ago,
            Product.highlight_items.any(
                ProductHighlightItem.highlight.has(ProductHighlight.type == "New Releases")
            )
        ))

    if category_id:
        final_ids = await get_active_subtree_ids(db, category_id)
        query = query.where(Product.category_id.in_(final_ids))
    if category_slug:
        # Support multiple comma-separated slugs
        cat_slug_list = [s.strip() for s in category_slug.split(",") if s.strip()]
        cat_ids_res = await db.execute(select(Category.id).where(Category.slug.in_(cat_slug_list)))
        found_cat_ids = cat_ids_res.scalars().all()
        if found_cat_ids:
            all_cat_ids = set()
            for cid in found_cat_ids:
                subtree = await get_active_subtree_ids(db, cid)
                all_cat_ids.update(subtree)
            query = query.where(Product.category_id.in_(list(all_cat_ids)))

    if brand_id:
        query = query.where(Product.brand_id == brand_id)
    if brand_slug:
        # Support multiple comma-separated slugs
        slug_list = [s.strip() for s in brand_slug.split(",") if s.strip()]
        if len(slug_list) == 1:
            query = query.where(Brand.slug == slug_list[0])
        else:
            query = query.where(Brand.slug.in_(slug_list))

    if vendor_id:
        query = query.where(Product.vendor_id == vendor_id)

    # --- Price Range Filter (e.g., "4,999-14,999") ---
    price_range_param = request.query_params.get("price_ranges")
    if price_range_param:
        try:
            import re

            # Find all occurrences of "<num>-<num>" where numbers may have commas and optional decimals
            matches = re.findall(
                r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)[\s]*-[\s]*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)",
                price_range_param,
            )

            ranges = []
            for a, b in matches:
                lo = float(a.replace(",", ""))
                hi = float(b.replace(",", ""))
                if lo > hi:
                    lo, hi = hi, lo
                ranges.append((lo, hi))

            if ranges:
                price_conditions = [
                    and_(Product.price >= lo, Product.price <= hi) for lo, hi in ranges
                ]
                query = query.where(or_(*price_conditions))

        except (ValueError, InvalidOperation, Exception) as e:
            logger.error(f"Price filter parse error: {e}")
    # --- Sorting ---
    if sort_by == "price_asc":
        query = query.order_by(Product.price.asc())

    if sort_by == "stock_asc":
        query = query.order_by(Product.stock.asc())

    if sort_by == "stock_desc":
        query = query.order_by(Product.stock.desc())

    elif sort_by == "price_desc":
        query = query.order_by(Product.price.desc())

    elif sort_by == "newly_added":
        query = query.order_by(Product.created_at.desc())

    elif sort_by == "oldest":
        query = query.order_by(Product.created_at.asc())

    elif sort_by == "top_rated":
        query = query.join(ReviewStats, ReviewStats.product_id == Product.id)
        query = query.order_by(
            func.coalesce(ReviewStats.average_rating, 0).desc(),
            func.coalesce(ReviewStats.total_reviews, 0).desc(),
        )
    elif sort_by == "biggest_saving":
        variant_saving_subq = (
            select(
                ProductVariant.product_id.label("product_id"),
                func.max(
                    (ProductVariant.rrp_price - ProductVariant.price)
                    / func.nullif(ProductVariant.rrp_price, 0)
                ).label("max_saving_percent"),
            )
            .where(
                ProductVariant.rrp_price.isnot(None),
                ProductVariant.price.isnot(None),
            )
            .group_by(ProductVariant.product_id)
            .subquery()
        )
        query = query.outerjoin(
            variant_saving_subq,
            variant_saving_subq.c.product_id == Product.id,
        ).order_by(
            func.coalesce(
                variant_saving_subq.c.max_saving_percent,
                (Product.rrp_price - Product.price) / func.nullif(Product.rrp_price, 0),
                0,
            ).desc()
        )

    # --- Unified Fuzzy Name Search (Product + Brand + Category) ---
    if name:
        # 2️⃣ Fuzzy match fallback for brand + category + product
        search_value = name.strip().lower()
        brand_ids = await get_fuzzy_matched_ids(db, Brand, Brand.name, search_value)
        category_ids = await get_fuzzy_matched_ids(
            db, Category, Category.name, search_value
        )
        product_ids = await get_fuzzy_matched_ids(
            db, Product, Product.title, search_value
        )

        # Combine fuzzy matches safely
        conditions = []
        if product_ids:
            conditions.append(Product.id.in_(product_ids))
        if brand_ids:
            conditions.append(Product.brand_id.in_(brand_ids))
        if category_ids:
            conditions.append(Product.category_id.in_(category_ids))

        if conditions:
            query = query.where(or_(*conditions))
    
    # --- Parse and apply custom filters (attributes / brand name / category name) ---
    standard_fields = {
        "sku",
        "name",
        "status",
        "min_price",
        "max_price",
        "category_id",
        "category_slug",
        "brand_id",
        "brand_slug",
        "sort_by",
        "page",
        "limit",
        "price_ranges",
        "free_shipping",
        "fast_dispatch",
        "product_condition",
        "test_id",  # Added to allow cache busting in tests without breaking filters
    }

    custom_filters = {
        k: v
        for k, v in request.query_params.items()
        if k.lower() not in standard_fields
    }

    for attr_name, attr_value in custom_filters.items():
        if not attr_value:
            continue  # skip empty filters

        # Match by Brand Name
        if attr_name in {"brand", "brand_name", "brands"} and attr_value:
            attr_value = unquote_plus(attr_value)
            search_values = [
                v.strip().lower() for v in attr_value.split(",") if v.strip()
            ]

            brand_subq = select(Brand.id).where(
                func.lower(Brand.name).in_(search_values)
            )
            query = query.where(Product.brand_id.in_(brand_subq))
            continue  # skip generic attribute filter for brand

        # Match by Category Name
        if attr_name in {"category", "category_name", "categories"} and attr_value:
            attr_value = unquote_plus(attr_value)
            search_values = [v.strip() for v in attr_value.split(",") if v.strip()]
            conditions = [func.lower(Category.name) == v.lower() for v in search_values]
            # fetch (id, parent_id) for matches
            rows = (
                await db.execute(
                    select(Category.id, Category.parent_id).where(or_(*conditions))
                )
            ).all()
            if not rows:
                continue

            # choose parent if exists, else self
            root_ids = {(pid if pid else cid) for (cid, pid) in rows}

            # expand to subtree (so children categories’ products also match)
            active_ids = await get_active_category_ids_cte(db)

            all_ids: set[str] = set(root_ids)
            for rid in root_ids:
                all_ids.update(await get_all_subcategory_ids_cte(db, rid))

            # filter products
            final_ids = list(set(all_ids) & active_ids)
            query = query.where(Product.category_id.in_(final_ids))

            continue  # skip generic attribute filter for category

        #  Custom attribute-based filters (search inside variant attributes)
        attr_value = unquote_plus(attr_value)
        search_values = [v.strip().lower() for v in attr_value.split(",") if v.strip()]
        if not search_values:
            continue
        attr_conditions = [
            func.lower(Attribute.value).ilike(f"%{val}%") for val in search_values
        ]
        query = query.where(
            Product.id.in_(
                select(ProductVariant.product_id)
                .join(Attribute, Attribute.variant_id == ProductVariant.id)
                .where(or_(*attr_conditions))
            )
        )

    logger.info(f"Filter building time: {time.time() - start_time:.4f}s")
    count_start = time.time()
    # --- Pagination / Count (count uses the query as subquery) ---
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total_count = total_result.scalar_one()
    logger.info(f"Count query time: {time.time() - count_start:.4f}s")

    offset = (page - 1) * limit
    paginated_query = query.limit(limit).offset(offset)

    main_query_start = time.time()
    result = await db.execute(paginated_query)
    products = result.scalars().unique().all()
    logger.info(f"Main query time: {time.time() - main_query_start:.4f}s")

    enrich_start = time.time()
    # --- Enrich products with promotion and review data ---
    enriched_products = []
    for product in products:
        logger.info(f"PRODUCT ID: {product.id}")
        logger.info(f"CATEGORY ID: {product.category_id}")
        promo = calculate_best_promotion(str(product.id), str(product.category_id), promotions)
        logger.info(f"promo {promo}")

        # Prepare Review Stats (Common)
        if product.review_stats:
            review_stats = {
                "average_rating": round(product.review_stats.average_rating, 2),
                "total_reviews": product.review_stats.total_reviews,
                "five_star_count": product.review_stats.five_star_count,
                "four_star_count": product.review_stats.four_star_count,
                "three_star_count": product.review_stats.three_star_count,
                "two_star_count": product.review_stats.two_star_count,
                "one_star_count": product.review_stats.one_star_count,
            }
        else:
            review_stats = {
                "average_rating": 0,
                "total_reviews": 0,
                "five_star_count": 0,
                "four_star_count": 0,
                "three_star_count": 0,
                "two_star_count": 0,
                "one_star_count": 0,
            }

        product_dict = jsonable_encoder(product)
        product_dict["brand_name"] = product.brand_name
        product_dict["brand_slug"] = product.brand_slug
        product_dict["category_name"] = product.category_name
        product_dict["category_slug"] = product.category_slug
        product_dict["unique_code"] = product.unique_code
        product_dict["review_stats"] = review_stats

        # --- Calculate Tags (Logic based on highlight types) ---
        tags = []
        highlight_types = [hi.highlight.type for hi in product.highlight_items] if product.highlight_items else []
        
        # Priority 1: Sale
        if promo or "Whats On Sale" in highlight_types:
            tags.append("Sale")
            # If it's a promotion, we also set sale_ends_at for countdown
            if promo:
                raw_promo = next((p for p in promotions if p.get("name") == promo.offer_name), None)
                if raw_promo and raw_promo.get("end_date"):
                    try:
                        product_dict["sale_ends_at"] = raw_promo.get("end_date")
                    except:
                        pass

        # Priority 2: Coupon
        if promo and "coupon" in promo.offer_name.lower():
            if "Coupon" not in tags: tags.append("Coupon")

        # Priority 3: Clearance
        if "Clearance" in highlight_types:
            tags.append("Clearance")

        # Priority 4: Bestseller
        if "Best Sellers" in highlight_types:
            tags.append("Bestseller")

        # Priority 5: New Arrival / New Releases
        if (product.created_at and (datetime.now(product.created_at.tzinfo) - product.created_at).days <= 15) or "New Releases" in highlight_types:
            tags.append("New Arrival")

        # Priority 6: Popular
        if "Popular" in highlight_types:
            tags.append("Popular")
        
        # Priority 7: Deals
        if any(h in highlight_types for h in ["Today's Deal", "Todays Deals", "Hot Deals", "Trending Deals"]):
            if "Trending Deals" in highlight_types:
                tags.append("Trending")
            elif "Hot Deals" in highlight_types:
                tags.append("Hot Deal")
            else:
                tags.append("Deal")

        # Priority 8: Top Rated
        if "Top Rated" in highlight_types:
            tags.append("Top Rated")

        # Priority 9: Price Drop
        if product.rrp_price and product.price and product.price < product.rrp_price:
            if product.created_at and (datetime.now(product.created_at.tzinfo) - product.created_at).days <= 15:
                tags.append("Price Drop")

        # Priority 10: Sold Counts
        total_orders = sum(s.orders for s in product.stats) if product.stats else 0
        if total_orders >= 10:
            tags.append(f"Sold {total_orders}+")
        
        # Sort tags by priority? (Sale=1, Coupon=2, ...)
        # The list is already appended in priority order above.
        product_dict["tags"] = tags

        # Sold Count
        total_orders = sum(s.orders for s in product.stats) if product.stats else 0
        product_dict["sold_count"] = total_orders
        
        # --- Handle Parent Images ---
        imgs = list(product.images or [])
        if imgs:
            imgs_sorted = sorted(
                imgs,
                key=lambda i: (
                    not bool(getattr(i, "is_main", False)),  # existing main first
                    getattr(i, "created_at", None) or getattr(i, "id", None),
                ),
            )
            first_img = imgs_sorted[0]
            product_dict["images"] = [
                {
                    "id": img.id,
                    "url": img.image_url,
                    "is_main": img.id == first_img.id, 
                    "video_url": getattr(img, "video_url", None),
                }
                for img in imgs_sorted
            ]
        else:
            product_dict["images"] = []

        # --- Handle Variants (Nested) ---
        enriched_variants = []
        if product.variants:
            for variant in product.variants:
                variant_item = jsonable_encoder(variant)
                
                # Variant Images
                v_imgs = list(variant.images or [])
                if v_imgs:
                    v_imgs_sorted = sorted(
                        v_imgs,
                        key=lambda i: (
                            not bool(getattr(i, "is_main", False)),
                            getattr(i, "created_at", None) or getattr(i, "id", None),
                        ),
                    )
                    v_first_img = v_imgs_sorted[0]
                    variant_item["images"] = [
                        {
                            "id": img.id,
                            "url": img.image_url,
                            "is_main": img.id == v_first_img.id,
                            "video_url": getattr(img, "video_url", None),
                        }
                        for img in v_imgs_sorted
                    ]
                else:
                    variant_item["images"] = []

                # Variant Promotion
                if promo:
                    variant_item["promotion_name"] = promo.offer_name
                    variant_item["discount_percentage"] = promo.discount_percentage
                    variant_item["discounted_price"] = calculate_discounted_price(
                        variant.price, promo.discount_percentage, promo.max_discount_amount
                    )
                else:
                    variant_item["promotion_name"] = None
                    variant_item["discount_percentage"] = 0
                    variant_item["discounted_price"] = 0
                
                enriched_variants.append(variant_item)

        product_dict["variants"] = enriched_variants

        # Parent Promotion
        if promo:
            product_dict["promotion_name"] = promo.offer_name
            product_dict["discount_percentage"] = promo.discount_percentage
            product_dict["discounted_price"] = calculate_discounted_price(
                product.price, promo.discount_percentage, promo.max_discount_amount
            )
        else:
            product_dict["promotion_name"] = None
            product_dict["discount_percentage"] = 0
            product_dict["discounted_price"] = 0

        enriched_products.append(product_dict)
    
    logger.info(f"Enrichment time: {time.time() - enrich_start:.4f}s")

    # --- PRODUCT FILTERS (Brand / Price / Variant attributes) ---
    filters = []

    sidebar_filters_start = time.time()
    # 1️⃣ Selection Context for sidebar filters
    context_cat_ids = []
    if category_id:
        context_cat_ids = await get_active_subtree_ids(db, category_id)
    elif category_slug:
        cat_slug_list = [s.strip() for s in category_slug.split(",") if s.strip()]
        cat_ids_res = await db.execute(select(Category.id).where(Category.slug.in_(cat_slug_list)))
        found_cat_ids = cat_ids_res.scalars().all()
        all_sub_ids = set()
        for cid in found_cat_ids:
            subtree = await get_active_subtree_ids(db, cid)
            all_sub_ids.update(subtree)
        context_cat_ids = list(all_sub_ids)

    # Base query for the selection context (e.g. all active products in category)
    base_context_q = select(Product.id).where(Product.status == "active")
    if context_cat_ids:
        base_context_q = base_context_q.where(Product.category_id.in_(context_cat_ids))
    
    # We don't join Brand/Category in base_context_q to keep filter queries simple
    context_ids_scalar_q = base_context_q.scalar_subquery()

    # --- 2️⃣ Brand Filter (all brands in current context) ---
    brand_count_q = (
        select(Brand.name)
        .join(Product, Product.brand_id == Brand.id)
        .where(Product.id.in_(context_ids_scalar_q))
        .where(Brand.is_active.is_(True))
        .distinct()
        .order_by(Brand.name)
    )
    brand_rows = await db.execute(brand_count_q)
    brand_names = [r[0] for r in brand_rows.fetchall() if r[0]]
    if brand_names:
        filters.append({"attribute": "Brand", "values": brand_names})
    
    # --- 3️⃣ Category Filter (Hierarchical - only categories with products in current context) ---
    category_count_q = (
        select(Product.category_id, func.count(Product.id))
        .where(Product.id.in_(context_ids_scalar_q))
        .group_by(Product.category_id)
    )
    category_counts_res = await db.execute(category_count_q)
    category_counts = {row[0]: row[1] for row in category_counts_res.fetchall()}

    all_categories = await get_all_categories_flat(db)
    category_tree = build_category_tree_fast(
        all_categories, category_counts, only_with_products=True
    )
    if category_tree:
        filters.append({"attribute": "Category", "values": category_tree})
    
    # --- 4️⃣ Price Filter (always scoped by current search/filter results) ---
    current_results_subq = query.with_only_columns(Product.id).subquery()
    await build_price_filter(db, current_results_subq, filters)

    # --- 5️⃣ Shipping Filter ---
    shipping_options = []
    
    # Check for Free Shipping
    free_ship_exists = await db.execute(
        select(Product.id).where(Product.id.in_(context_ids_scalar_q), Product.free_shipping == True).limit(1)
    )
    if free_ship_exists.scalar():
        shipping_options.append("Free Shipping")
        
    # Check for Fast Dispatch
    fast_dispatch_exists = await db.execute(
        select(Product.id).where(Product.id.in_(context_ids_scalar_q), Product.fast_dispatch == True).limit(1)
    )
    if fast_dispatch_exists.scalar():
        shipping_options.append("Fast Dispatch")
        
    if shipping_options:
        filters.append({"attribute": "Shipping", "values": sorted(shipping_options)})

    # --- 6️⃣ Condition Filter ---
    condition_count_q = (
        select(Product.product_condition)
        .where(Product.id.in_(context_ids_scalar_q))
        .where(Product.product_condition.isnot(None))
        .distinct()
        .order_by(Product.product_condition)
    )
    condition_rows = await db.execute(condition_count_q)
    conditions = [r[0] for r in condition_rows.fetchall() if r[0]]
    if conditions:
        filters.append({"attribute": "Condition", "values": sorted(conditions)})

    # --- 6.1️⃣ Special Offers Sidebar Filter ---
    special_offers = []
    
    # helper for checking existence in current context
    async def check_highlight_exists(types: list[str]) -> bool:
        stmt = select(Product.id).join(ProductHighlightItem).join(ProductHighlight).where(
            Product.id.in_(context_ids_scalar_q),
            ProductHighlight.type.in_(types),
            Product.status == "active"
        ).limit(1)
        return (await db.execute(stmt)).scalar() is not None

    if await check_highlight_exists(["Today's Deal", "Todays Deals"]):
        special_offers.append("Today's Special")
    
    # Sales check
    now = datetime.now()
    from models.promotions import Promotion
    sales_stmt = select(Product.id).where(
        Product.id.in_(context_ids_scalar_q),
        Product.id.in_(
            select(Promotion.reference_id).where(
                Promotion.offer_type == 'product',
                Promotion.status == 'active',
                Promotion.start_date <= now,
                Promotion.end_date >= now
            )
        )
    ).limit(1)
    if (await db.execute(sales_stmt)).scalar():
        special_offers.append("Sales")
        
    if await check_highlight_exists(["Clearance"]):
        special_offers.append("Clearance")
        
    if await check_highlight_exists(["Hot Deals", "Trending Deals"]):
        special_offers.append("Limited Time Deal")
        
    # New Arrivals check
    fifteen_days_ago = now - timedelta(days=15)
    new_arrivals_stmt = select(Product.id).where(
        Product.id.in_(context_ids_scalar_q),
        or_(
            Product.created_at >= fifteen_days_ago,
            Product.id.in_(
                select(ProductHighlightItem.product_id).join(ProductHighlight).where(
                    ProductHighlight.type == "New Releases"
                )
            )
        )
    ).limit(1)
    if (await db.execute(new_arrivals_stmt)).scalar():
        special_offers.append("New Arrivals")

    if special_offers:
        filters.append({"attribute": "Special Offers", "values": special_offers})

    # --- 7️⃣ Variant Attributes (only when a context is selected to avoid clutter) ---
    if any([category_id, category_slug, name]):
        variant_attr_query = (
            select(Attribute.name, Attribute.value)
            .join(ProductVariant, Attribute.variant_id == ProductVariant.id)
            .where(ProductVariant.product_id.in_(context_ids_scalar_q))
        )
        attr_result = await db.execute(variant_attr_query)
        variant_attrs = attr_result.fetchall()

        def normalize_filter_value(v: str) -> str:
            if not v:
                return v
            # Normalize separators without ReDoS risk: " / " -> "/", " & " -> " & "
            v = "/".join(part.strip() for part in v.split("/"))
            v = " & ".join(part.strip() for part in v.split("&"))
            # Collapse multiple spaces and strip
            v = " ".join(v.split())
            # Consistent title casing
            return v.title()

        attribute_map = defaultdict(set)
        for name_attr, value_attr in variant_attrs:
            if name_attr and value_attr:
                # Normalize name (e.g. "color" -> "Color")
                normalized_name = normalize_filter_value(name_attr)
                # Normalize value (e.g. "black/ grey" -> "Black/Grey")
                normalized_value = normalize_filter_value(value_attr)
                attribute_map[normalized_name].add(normalized_value)

        for attr_name, values in attribute_map.items():
            filters.append({"attribute": attr_name, "values": sorted(list(values))})

    logger.info(f"Filters built: {len(filters)} attributes found")
    logger.info(f"Sidebar filters time: {time.time() - sidebar_filters_start:.4f}s")
    
    sort_start = time.time()
    # Global sorting for human-friendly output
    for f in filters:
        if f["attribute"].lower() == "price":
            f["values"] = sorted(f["values"], key=price_sort_key)
        elif f["attribute"].lower() == "category":
            # For hierarchical categories, sort by name
            f["values"] = sorted(f["values"], key=lambda x: natural_sort_key(x["name"]))
        else:
            f["values"] = sorted(f["values"], key=natural_sort_key)

    # --- Return response ---
    response_data = {
        "page": page,
        "limit": limit,
        "total": total_count,
        "pages": (total_count + limit - 1) // limit,
        "data": enriched_products,
        "filters": filters,
    }

    # Cache the result (expire in 5 minutes)
    # try:
    #     await redis_client.setex(
    #         cache_key,
    #         300,
    #         json.dumps(jsonable_encoder(response_data))
    #     )
    # except Exception as e:
    #     logger.error(f"Redis set error: {e}")
        
    logger.info(f"Total time _get_products_list: {time.time() - start_time:.4f}s")
    return response_data


# ---------------- LIST PRODUCTS ----------------
@router.get("/list-products", response_model=PaginatedProductResponse)
async def list_products(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    sku: Annotated[str | None, Query(description="Filter by SKU")] = None,
    name: Annotated[str | None, Query(description="Filter by product name")] = None,
    min_price: Annotated[float | None, Query(description="Filter by minimum price")] = None,
    max_price: Annotated[float | None, Query(description="Filter by maximum price")] = None,
    category_id: Annotated[str | None, Query(description="Filter by category ID")] = None,
    category_slug: Annotated[str | None, Query(description="Filter by category slug")] = None,
    brand_id: Annotated[str | None, Query(description="Filter by brand ID")] = None,
    brand_slug: Annotated[str | None, Query(description="Filter by brand slug")] = None,
    sort_by: Annotated[
        str | None,
        Query(
            regex="^(price_asc|price_desc|newly_added|top_rated|biggest_saving|oldest|stock_asc|stock_desc)$",
            description=(
                "Sort products by: "
                "'price_asc', 'price_desc', 'newly_added', 'top_rated', 'biggest_saving','oldest','stock_asc','stock_desc'"
            ),
        ),
    ] = "newly_added",
    fast_dispatch: Annotated[bool | None, Query(description="Filter by fast dispatch status")] = None,
    product_condition: Annotated[str | None, Query(description="Filter by product condition (e.g., 'New', 'Used')")] = None,
    page: Annotated[int, Query(ge=1, description="Page number")] = 1,
    limit: Annotated[int, Query(ge=1, le=100, description="Number of items per page")] = 10,
    free_shipping: Annotated[bool | None, Query(description="Filter by free shipping status")] = None,
    vendor_id: Annotated[str | None, Query(description="Filter by vendor ID")] = None,
    todays_special: Annotated[bool | None, Query(description="Today's Special offer")] = None,
    sales: Annotated[bool | None, Query(description="Items on Sale")] = None,
    clearance: Annotated[bool | None, Query(description="Clearance items")] = None,
    limited_time_deal: Annotated[bool | None, Query(description="Limited Time Deal")] = None,
    new_arrivals: Annotated[bool | None, Query(description="New Arrivals")] = None,
):
    """
    List all products with optional filters, sorting, and pagination.
    Includes variants and their attributes.
    """
    return await _get_products_list(
        request=request,
        db=db,
        sku=sku,
        name=name,
        min_price=min_price,
        max_price=max_price,
        category_id=category_id,
        category_slug=category_slug,
        brand_id=brand_id,
        brand_slug=brand_slug,
        sort_by=sort_by,
        page=page,
        limit=limit,
        free_shipping=free_shipping,
        fast_dispatch=fast_dispatch,
        product_condition=product_condition,
        promotions_only=False,
        vendor_id=vendor_id,
        todays_special=todays_special,
        sales=sales,
        clearance=clearance,
        limited_time_deal=limited_time_deal,
        new_arrivals=new_arrivals
    )


@router.get("/list-promotional-products", response_model=PaginatedProductResponse)
async def list_promotional_products(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    sku: Annotated[str | None, Query(description="Filter by SKU")] = None,
    name: Annotated[str | None, Query(description="Filter by product name")] = None,
    min_price: Annotated[float | None, Query(description="Filter by minimum price")] = None,
    max_price: Annotated[float | None, Query(description="Filter by maximum price")] = None,
    category_id: Annotated[str | None, Query(description="Filter by category ID")] = None,
    category_slug: Annotated[str | None, Query(description="Filter by category slug")] = None,
    brand_id: Annotated[str | None, Query(description="Filter by brand ID")] = None,
    brand_slug: Annotated[str | None, Query(description="Filter by brand slug")] = None,
    sort_by: Annotated[
        str | None,
        Query(
            regex="^(price_asc|price_desc|newly_added|top_rated|biggest_saving|oldest|stock_asc|stock_desc)$",
            description=(
                "Sort products by: "
                "'price_asc', 'price_desc', 'newly_added', 'top_rated', 'biggest_saving','oldest','stock_asc','stock_desc'"
            ),
        ),
    ] = "newly_added",
    page: Annotated[int, Query(ge=1, description="Page number")] = 1,
    limit: Annotated[int, Query(ge=1, le=100, description="Number of items per page")] = 10,
    free_shipping: Annotated[bool | None, Query(description="Filter by free shipping status")] = None,
    fast_dispatch: Annotated[bool | None, Query(description="Filter by fast dispatch status")] = None,
    product_condition: Annotated[str | None, Query(description="Filter by product condition")] = None,
    vendor_id: Annotated[str | None, Query(description="Filter by vendor ID")] = None,
    todays_special: Annotated[bool | None, Query(description="Today's Special offer")] = None,
    sales: Annotated[bool | None, Query(description="Items on Sale")] = None,
    clearance: Annotated[bool | None, Query(description="Clearance items")] = None,
    limited_time_deal: Annotated[bool | None, Query(description="Limited Time Deal")] = None,
    new_arrivals: Annotated[bool | None, Query(description="New Arrivals")] = None,
):
    """
    List all products that have active promotions applied. 
    Identical to /list-products but filtered for promotional items.
    """
    logger.info("list_promotional_products")

    return await _get_products_list(
        request=request,
        db=db,
        sku=sku,
        name=name,
        min_price=min_price,
        max_price=max_price,
        category_id=category_id,
        category_slug=category_slug,
        brand_id=brand_id,
        brand_slug=brand_slug,
        sort_by=sort_by,
        page=page,
        limit=limit,
        free_shipping=free_shipping,
        fast_dispatch=fast_dispatch,
        product_condition=product_condition,
        promotions_only=True,
        vendor_id=vendor_id,
        todays_special=todays_special,
        sales=sales,
        clearance=clearance,
        limited_time_deal=limited_time_deal,
        new_arrivals=new_arrivals
    )


# ---------------- GET SINGLE PRODUCT ----------------


@router.get(
    "/get-product/{unique_code}",
    response_model=ProductResponse,
    responses={
        404: {"description": "Product not found"},
    },
)
async def get_product(
    unique_code: str,
    response: Response,
    db: Annotated[AsyncSession, Depends(get_db)],
    country: Annotated[Optional[str], Query(min_length=2, max_length=2)] = None,
    is_admin: Annotated[bool, Query()] = False,
    request: Request = None,
):
    product_id = unique_code
    start_time = time.time()
    
    # 1. Try to find if the ID is a Variant ID first
    lookup_start = time.time()
    variant_query = select(ProductVariant).where(ProductVariant.id == product_id)
    variant_result = await db.execute(variant_query)
    target_variant = variant_result.scalar_one_or_none()
    logger.info(f"Variant Lookup Check | Time: {time.time() - lookup_start:.4f}s")
    
    real_product_id = product_id
    is_variant_request = False
    
    if target_variant:
        real_product_id = target_variant.product_id
        is_variant_request = True
    
    # Identify user or session (for guests)
    user_id = getattr(request.state, "user_id", None)  # if using JWT middleware
    session_id = request.cookies.get("session_id")

    if not session_id:
        session_id = str(uuid.uuid4())
    
    # --- Check Active Status (Before Cache) ---
    status_start = time.time()
    status_query = select(Product.id, Product.status).where(
        or_(
            Product.id == real_product_id,
            Product.unique_code == real_product_id,
            Product.slug == real_product_id
        )
    )
    status_result = await db.execute(status_query)
    product_row = status_result.first()
    logger.info(f"Product Status Check | Time: {time.time() - status_start:.4f}s")

    if not product_row:
         raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Product not found"
        )
    
    real_product_id = product_row.id
    current_status = product_row.status
    
    if not is_admin and current_status != "active":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Product not found"
        )

    # We need product_id for RecentlyViewed, ensuring we have it from data or params
    # product_id param is safer
    recent_view_start = time.time()
    await db.execute(
        insert(RecentlyViewed).values(
            product_id=real_product_id, # Always log against the PARENT product ID
            # user_id=user_id,
            session_id=session_id,
        )
    )
    # await record_product_activity(db, product_id, view=True)
    await db.commit()
    logger.info(f"Recently Viewed Log | Time: {time.time() - recent_view_start:.4f}s")

    # --- Redis Cache Check ---
    redis_client = redis.from_url(get_redis_url())
    # Cache key distinguishing variant request
    cache_key = f"product_detail:{product_id}:{is_admin}"
    
    product_data = None
    # try:
    #     cache_start = time.time()
    #     cached = await redis_client.get(cache_key)
    #     if cached:
    #         product_data = json.loads(cached)
    #         logger.info(f"Redis Cache Hit | Time Taken: {time.time() - cache_start:.4f}s")
    #     else:
    #         logger.info(f"Redis Cache Miss | Time Taken: {time.time() - cache_start:.4f}s")
    # except Exception as e:
    #     logger.error(f"Redis get error: {e}")

    if not product_data:
        db_start = time.time()
        cte_start = time.time()
        active_category_ids = await get_active_category_ids_cte(db)
        logger.info(f"Category CTE | Time: {time.time() - cte_start:.4f}s")
        
        # Prepare query conditions (always query by Parent ID)
        conditions = [Product.id == real_product_id]
        if not is_admin:
            conditions.append(Product.status == "active")
            conditions.append(Product.category.has(Category.is_active == True))
            conditions.append(Product.brand.has(Brand.is_active == True))

        result = await db.execute(
            select(Product)
            .options(
                selectinload(Product.variants).selectinload(ProductVariant.attributes),
                selectinload(Product.variants).selectinload(ProductVariant.images), # Ensure variant images load
                selectinload(Product.review_stats),
                selectinload(Product.brand),
                selectinload(Product.reviews),
                selectinload(Product.images),
                selectinload(Product.category),
                selectinload(Product.seo),
                selectinload(Product.highlight_items).selectinload(ProductHighlightItem.highlight),
                selectinload(Product.stats),
            )
            .where(*conditions)
        )
        logger.info(f"DB Query Execution | Time: {time.time() - db_start:.4f}s")

        product = result.scalar_one_or_none()

        if not product:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Product not found"
            )

        # Basic serialization of PARENT
        serialize_start = time.time()
        product_data = ProductResponse.model_validate(product).model_dump(mode="json")
        product_data["brand_name"] = product.brand_name
        product_data["brand_slug"] = product.brand_slug
        product_data["category_name"] = product.category_name
        product_data["category_slug"] = product.category_slug
        product_data["unique_code"] = product.unique_code
        
        # logger.info(f"Serialization | Time: {time.time() - serialize_start:.4f}s")
        
        # --- Bundle Products ---
        bundle_start = time.time()
        bundle_products = await build_bundle_products(db, product)
        product_data["bundle_products"] = bundle_products
        logger.info(f"Bundle Products | Time: {time.time() - bundle_start:.4f}s")

        # --- Images Setup ---
        images_start = time.time()
        
        # If accessing via Variant ID, we FLATTEN the response
        if is_variant_request and target_variant:
            # Override Parent fields with Variant fields
            # Use jsonable_encoder to ensure Decimals are converted
            variant_data = jsonable_encoder(target_variant)
            
            product_data["id"] = variant_data["id"]
            product_data["sku"] = variant_data["sku"]
            product_data["title"] = f"{product.title}"
            product_data["price"] = variant_data["price"]
            product_data["cost_price"] = variant_data["cost_price"]
            product_data["rrp_price"] = variant_data["rrp_price"]
            product_data["stock"] = variant_data["stock"]
            product_data["width"] = variant_data["width"]
            product_data["height"] = variant_data["height"]
            product_data["length"] = variant_data["length"]
            product_data["weight"] = variant_data["weight"]
            product_data["ean"] = variant_data["ean"]
            product_data["hs_code"] = variant_data["hs_code"]
            
            # Metadata keys
            product_data["is_variant"] = True
            product_data["variant_title"] = target_variant.title
            product_data["parent_product_id"] = product.id
            
            # Variant Images
            # We need to find the variant object in the loaded product.variants to get images safely
            # (or use target_variant if we eager loaded it, but select(ProductVariant) above didn't eager load images)
            # Better to find it in product.variants which WAS eager loaded.
            loaded_variant = next((v for v in product.variants if v.id == target_variant.id), None)
            
            v_imgs = list(loaded_variant.images or []) if loaded_variant else []
            
            if v_imgs:
                imgs_sorted = sorted(
                    v_imgs,
                    key=lambda i: (
                        not bool(getattr(i, "is_main", False)),
                        getattr(i, "created_at", None) or getattr(i, "id", None),
                    ),
                )
                first_img = imgs_sorted[0]
                product_data["images"] = [
                    {
                        "id": img.id,
                        "image_url": getattr(img, "image_url", None),
                        "image_order": getattr(img, "image_order", 0),
                        "is_main": img.id == first_img.id,
                        "video_url": getattr(img, "video_url", None),
                    }
                    for img in imgs_sorted
                ]
            else:
                 # Fallback to Parent Images
                p_imgs = list(product.images or [])
                if p_imgs:
                     imgs_sorted = sorted(
                        p_imgs,
                        key=lambda i: (
                            not bool(getattr(i, "is_main", False)),
                            getattr(i, "created_at", None) or getattr(i, "id", None),
                        ),
                    )
                     first_img = imgs_sorted[0]
                     product_data["images"] = [
                        {
                            "id": img.id,
                            "image_url": getattr(img, "image_url", None),
                            "image_order": getattr(img, "order", 0),
                            "is_main": img.id == first_img.id,
                            "video_url": getattr(img, "video_url", None),
                        }
                        for img in imgs_sorted
                    ]
                else:
                    product_data["images"] = []

        else:
            # Standard Parent Product Response
            product_data["is_variant"] = False
            product_data["variant_title"] = None
            product_data["parent_product_id"] = None
            
            imgs = list(product.images or [])
            if imgs:
                imgs_sorted = sorted(
                    imgs,
                    key=lambda i: (
                        not bool(getattr(i, "is_main", False)),
                        getattr(i, "created_at", None) or getattr(i, "id", None),
                    ),
                )
                first_img = imgs_sorted[0]
                product_data["images"] = [
                    {
                        "id": img.id,
                        "image_url": getattr(img, "image_url", None),
                        "image_order": getattr(img, "image_order", 0),
                        "is_main": img.id == first_img.id,
                        "video_url": getattr(img, "video_url", None),
                    }
                    for img in imgs_sorted
                ]
            else:
                product_data["images"] = []
        
        logger.info(f"Images Processing | Time: {time.time() - images_start:.4f}s")
        
        # --- Populate "variants" list ---
        # IMPORTANT: We ALWAYS return the list of variants (siblings) so the frontend can choose them.
        # Even if we returned a flattened variant, the user needs to see other options.
        
        # We can reuse the `variants` list already present in `product_data` (from jsonable_encoder(product))
        # but we might want to ensure it has images or specific formatting if needed.
        # The default jsonable_encoder might not format `variants` list exactly as `ProductResponse` expects for nested items?
        # Actually `ProductResponse` has `variants: List[VariantResponse]`.
        # `jsonable_encoder(product)` should have included `variants` because of `selectinload`.
        
        # However, `VariantResponse` schema has `images: List[VariantImageResponse]`.
        # `jsonable_encoder` handles this if the model relationships match.
        # Let's ensure the `images` inside `variants` list are formatted or at least present.
        
        # Re-map variants list to ensure it matches schema and includes images correctly
        formatted_variants = []
        for v in product.variants:
             v_dict = jsonable_encoder(v)
             # Manual image fixup if needed? jsonable_encoder usually handles list of Pydantic models/ORM objs fine.
             # But let's be safe and ensure images structure is what frontend expects (url vs image_url aliases etc)
             
             # The `VariantResponse` schema expects `images` list.
             # ORM has `images`.
             
             # One detail: existing code (before my change) just did:
             # `product_data = jsonable_encoder(product)` 
             # and `product_data["variants"]` would be populated.
             # So we probably don't need to manually rebuild it unless we want to sort images inside each variant.
             pass
        
        # We DO need to make sure `variants` is populated in `product_data`.
        # If `jsonable_encoder(product)` was called, it should be there.
        # But if we OVERWROTE `product_data` keys when flattening (like `id`), the `variants` key should still be there from the original `product_data`.
        # Result: `product_data` has `variants` list (siblings). Perfect.

        # --- Adjust Stock for Reservations (Locked Inventory) ---
        variant_ids = [v.id for v in product.variants] if product.variants else []
        all_ids = [product.id] + variant_ids
        
        if all_ids:
            reservations_query = select(StockReservation.product_id, func.sum(StockReservation.quantity)).where(
                StockReservation.product_id.in_(all_ids),
                StockReservation.status == "active"
            ).group_by(StockReservation.product_id)
            
            res_stock = await db.execute(reservations_query)
            locked_stock_map = {row[0]: int(row[1]) if row[1] else 0 for row in res_stock.all()}
            
            # Adjust primary returned stock (which could be parent or flattened variant)
            current_id = product_data.get("id")
            if product_data.get("stock") is not None:
                product_data["stock"] = max(0, product_data["stock"] - locked_stock_map.get(current_id, 0))
                
            # Adjust variants list
            if product_data.get("variants"):
                for v_data in product_data["variants"]:
                    if v_data.get("stock") is not None:
                        v_data["stock"] = max(0, v_data["stock"] - locked_stock_map.get(v_data.get("id"), 0))

        # --- Return Policy ---
        policy_start = time.time()
        policy = await resolve_return_policy(
            db,
            product_id=product.id,
            country=(country or None),
        )
        product_data["return_policy"] = policy
        logger.info(f"Return Policy | Time: {time.time() - policy_start:.4f}s")

        # --- Promotion ---
        promo_start = time.time()
        promotions = await fetch_applicable_promotions()
        logger.info(f"Promotions: {promotions}")
        logger.info(f"PRODUCT ID: {product.id}")
        logger.info(f"CATEGORY ID: {product.category_id}")
        promo = calculate_best_promotion(str(product.id), str(product.category_id), promotions)
        
        # Note: If it's a variant request, we might want to check variant-specific promotion?
        # Current logic checks `get_applicable_promotion` which conceptually typically checks product or category.
        # If we need variant specific logic:
        # --- Calculate Tags (Logic based on highlight types) ---
        tags = []
        highlight_types = [hi.highlight.type for hi in product.highlight_items] if product.highlight_items else []
        
        # Priority 1: Sale
        if promo or "Whats On Sale" in highlight_types:
            tags.append("Sale")
            # If it's a promotion, we also set sale_ends_at for countdown
            if promo:
                raw_promo = next((p for p in promotions if p.get("name") == promo.offer_name), None)
                if raw_promo and raw_promo.get("end_date"):
                    try:
                        product_data["sale_ends_at"] = raw_promo.get("end_date")
                    except:
                        pass

        # Priority 2: Coupon
        if promo and "coupon" in promo.offer_name.lower():
            if "Coupon" not in tags: tags.append("Coupon")

        # Priority 3: Clearance
        if "Clearance" in highlight_types:
            tags.append("Clearance")

        # Priority 4: Bestseller
        if "Best Sellers" in highlight_types:
            tags.append("Bestseller")

        # Priority 5: New Arrival / New Releases
        if (product.created_at and (datetime.now(product.created_at.tzinfo) - product.created_at).days <= 15) or "New Releases" in highlight_types:
            tags.append("New Arrival")

        # Priority 6: Popular
        if "Popular" in highlight_types:
            tags.append("Popular")
        
        # Priority 7: Deals
        if any(h in highlight_types for h in ["Today's Deal", "Todays Deals", "Hot Deals", "Trending Deals"]):
            if "Trending Deals" in highlight_types:
                tags.append("Trending")
            elif "Hot Deals" in highlight_types:
                tags.append("Hot Deal")
            else:
                tags.append("Deal")

        # Priority 8: Top Rated
        if "Top Rated" in highlight_types:
            tags.append("Top Rated")

        # Priority 9: Price Drop
        if product.rrp_price and product.price and product.price < product.rrp_price:
            if product.created_at and (datetime.now(product.created_at.tzinfo) - product.created_at).days <= 15:
                tags.append("Price Drop")

        # Priority 10: Sold Counts
        total_orders = sum(s.orders for s in product.stats) if product.stats else 0
        if total_orders >= 10:
            tags.append(f"Sold {total_orders}+")
        
        # Sort tags by priority? (Sale=1, Coupon=2, ...)
        # The list is already appended in priority order above.
        product_data["tags"] = tags

        # Sold Count
        total_orders = sum(s.orders for s in product.stats) if product.stats else 0
        product_data["sold_count"] = total_orders

        # --- Promotion ---
        if promo:
            product_data["promotion_name"] = promo.offer_name
            product_data["discount_percentage"] = promo.discount_percentage
            
            # If variant request, calculate discount on VARIANT price
            target_price = product_data["price"] # This is already Variant Price if flattened, or Parent Price if not
            
            product_data["discounted_price"] = calculate_discounted_price(
                target_price, promo.discount_percentage, promo.max_discount_amount
            )
        else:
            product_data["promotion_name"] = None
            product_data["discount_percentage"] = 0
            product_data["discounted_price"] = 0
        logger.info(f"Promotion Calc | Time: {time.time() - promo_start:.4f}s")

        # --- Review Stats ---
        stats_start = time.time()
        if product.review_stats:
            product_data["review_stats"] = {
                "average_rating": product.review_stats.average_rating,
                "total_reviews": product.review_stats.total_reviews,
                "five_star_count": product.review_stats.five_star_count,
                "four_star_count": product.review_stats.four_star_count,
                "three_star_count": product.review_stats.three_star_count,
                "two_star_count": product.review_stats.two_star_count,
                "one_star_count": product.review_stats.one_star_count,
            }
        else:
            product_data["review_stats"] = {
                "average_rating": 0,
                "total_reviews": 0,
                "five_star_count": 0,
                "four_star_count": 0,
                "three_star_count": 0,
                "two_star_count": 0,
                "one_star_count": 0,
            }
        
        product_data["reviews"] = jsonable_encoder(product.reviews)
        logger.info(f"Review Stats | Time: {time.time() - stats_start:.4f}s")

        # # Cache the valid product data
        # try:
        #     await redis_client.setex(
        #         cache_key,
        #         60,
        #         json.dumps(product_data)
        #     )
        # except Exception as e:
        #     logger.error(f"Redis set error: {e}")
        
        logger.info(f"DB Fetch & Process | Time Taken: {time.time() - db_start:.4f}s")

    # await record_product_activity(db, product_id, view=True)

    response.set_cookie(
        key="session_id",
        value=session_id,
        httponly=True,
        max_age=60 * 60 * 24 * 30,  # 30 days
        samesite="Lax",
        path="/",
    )
    logger.info(f"Total Request Time: {time.time() - start_time:.4f}s")
    return product_data


# ---------------- UPDATE PRODUCT ----------------
@router.put(
    "/update-product/{product_id}",
    response_model=ProductResponseWithImages,
    responses={
        400: {"description": "Validation error (SKU/EAN exists, etc.)"},
        404: {"description": "Product or variant not found"},
    },
)
async def update_product(
    product_id: str,
    updates: ProductUpdateWithImages,
    db: Annotated[AsyncSession, Depends(get_db)],
    user: Annotated[dict, Depends(require_catalog_supervisor)],
):
    original_id = product_id.strip()
    product_id = original_id
    
    # Try to resolve variant first
    variant_match = (await db.execute(
        select(ProductVariant)
        .where(or_(ProductVariant.id == product_id, ProductVariant.sku == product_id))
    )).scalar_one_or_none()
    
    target_variant = None
    if variant_match:
        target_variant = variant_match
        product_id = target_variant.product_id # Switch context to parent product

    # fetch product with relations
    result = await db.execute(
        select(Product)
        .options(
            selectinload(Product.variants).selectinload(ProductVariant.attributes),
            selectinload(Product.variants).selectinload(ProductVariant.images),
            selectinload(Product.images),
            selectinload(Product.review_stats),
            selectinload(Product.reviews),
            selectinload(Product.brand),
            selectinload(Product.category),
            selectinload(Product.seo),
        )
        .where(or_(Product.id == product_id, Product.sku == product_id))
    )
    product = result.scalar_one_or_none()

    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Product or variant not found"
        )

    # Field Mappings
    variant_fields = {
        "title",
        "rrp_price",
        "price",
        "cost_price",
        "bundle_group_code",
        "images",
        "ean",
        "stock",
        "sku",
        "width",
        "height",
        "length",
        "weight",
    }
    
    # Split Updates
    payload_data = updates.dict(exclude_unset=True, exclude={"seo"})
    
    if target_variant:
        # TARGETED VARIANT UPDATE MODE
        # 1. Update Variant Specifics
        for field in variant_fields:
            if field in payload_data and field != "images":
                val = payload_data[field]
                if val != "":
                    # Special check for variant SKU/EAN uniqueness if changed
                    if field in ["sku", "ean"] and val != getattr(target_variant, field):
                         existing = (await db.execute(
                             select(ProductVariant).where(getattr(ProductVariant, field) == val, ProductVariant.id != target_variant.id)
                         )).scalar_one_or_none()
                         if existing:
                             raise HTTPException(status_code=400, detail=f"Variant {field.upper()} '{val}' already exists.")
                    setattr(target_variant, field, val)
        
        # 1.1 Update Attributes for targeted variant
        if updates.variants:
             # Look for the variant data in the list that matches the targeted variant (id or sku)
             v_data = next((v for v in updates.variants if v.id == target_variant.id or (v.sku and v.sku == target_variant.sku)), None)
             if v_data and v_data.attributes is not None:
                  # Wipe and recreate attributes for this variant
                  await db.execute(delete(Attribute).where(Attribute.variant_id == target_variant.id))
                  for attr in v_data.attributes:
                      db.add(Attribute(name=attr.name, value=attr.value, variant_id=target_variant.id))
        
        # 2. Update Variant Images
        if "images" in payload_data:
             # delete target variant's old images
             await db.execute(ProductVariantImage.__table__.delete().where(ProductVariantImage.variant_id == target_variant.id))
             # add new ones
             for img_data in payload_data["images"]:
                 img_url = await download_and_upload_image(img_data["image_url"], identifier=target_variant.sku)
                 db.add(ProductVariantImage(
                     variant_id=target_variant.id,
                     image_url=img_url,
                     is_main=img_data.get("is_main", False),
                     image_order=img_data.get("image_order", 0),
                     video_url=img_data.get("video_url")
                 ))
             
             # Sync variant.image_url cache field
             first_img = next((i for i in payload_data["images"] if i.get("is_main")), payload_data["images"][0])
             if first_img:
                  target_variant.image_url = await download_and_upload_image(first_img["image_url"], identifier=target_variant.sku)
        
        # 3. Update Parent with Shared Details
        for field, value in payload_data.items():
            if field not in variant_fields and field not in ["variants", "images", "seo"]:
                if value != "":
                    setattr(product, field, value)
                    
    else:
        # GLOBAL PRODUCT UPDATE MODE (Legacy behavior)
        # apply simple field updates (exclude variants & images)
        for field, value in updates.dict(
            exclude_unset=True, exclude={"variants", "images", "seo"}
        ).items():
            if value == "":
                continue
            if field == "ean" and value:
                res = await db.execute(
                    select(Product).where(Product.ean == value, Product.id != product.id)
                )
                if res.scalar_one_or_none():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="EAN already exists for another product")
            if field == "sku" and value:
                res = await db.execute(
                    select(Product).where(Product.sku == value, Product.id != product.id)
                )
                if res.scalar_one_or_none():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Product SKU '{value}' already exists.")
            setattr(product, field, value)

        # Sync Variants (Wipe and recreate legacy behavior)
        if updates.variants is not None:
             # Logic to handle variant updates as a whole list
             # ... (keep existing implementation for full list sync) ...
            for variant in product.variants:
                await db.execute(Attribute.__table__.delete().where(Attribute.variant_id == variant.id))
            await db.execute(ProductVariantImage.__table__.delete().where(ProductVariantImage.variant_id.in_(
                select(ProductVariant.id).where(ProductVariant.product_id == product.id).scalar_subquery()
            )))
            await db.execute(ProductVariant.__table__.delete().where(ProductVariant.product_id == product.id))

            for variant_data in updates.variants:
                existing_v = (await db.execute(select(ProductVariant).where(ProductVariant.sku == variant_data.sku))).scalar_one_or_none()
                if existing_v:
                    raise HTTPException(status_code=400, detail=f"Variant SKU '{variant_data.sku}' already exists.")
                
                new_v = ProductVariant(
                    product_id=product.id,
                    sku=variant_data.sku,
                    title=variant_data.title,
                    price=variant_data.price,
                    cost_price=variant_data.cost_price,
                    rrp_price=variant_data.rrp_price,
                    stock=variant_data.stock,
                    hs_code=variant_data.hs_code,
                    ean=variant_data.ean,
                    bundle_group_code=variant_data.bundle_group_code,
                    ships_from_location=variant_data.ships_from_location,
                    handling_time_days=variant_data.handling_time_days,
                    width=variant_data.width,
                    height=variant_data.height,
                    length=variant_data.length,
                    weight=variant_data.weight,
                )
                db.add(new_v)
                await db.flush()

                for attr in getattr(variant_data, "attributes", []) or []:
                    db.add(Attribute(name=attr.name, value=attr.value, variant_id=new_v.id))

                first_img = None
                for vimg in getattr(variant_data, "images", []) or []:
                    img_url = await download_and_upload_image(vimg.image_url, identifier=new_v.sku)
                    db.add(ProductVariantImage(
                        variant_id=new_v.id,
                        image_url=img_url,
                        is_main=getattr(vimg, "is_main", False),
                        image_order=getattr(vimg, "image_order", 0),
                        video_url=getattr(vimg, "video_url", None)
                    ))
                    if first_img is None: first_img = img_url
                if first_img: new_v.image_url = first_img

        # Product-level images
        if getattr(updates, "images", None) is not None:
            await db.execute(ProductImage.__table__.delete().where(ProductImage.product_id == product.id))
            for img_data in updates.images:
                img_url = await download_and_upload_image(img_data.image_url, identifier=product.sku)
                db.add(ProductImage(
                    product_id=product.id,
                    image_url=img_url,
                    is_main=getattr(img_data, "is_main", False),
                    image_order=getattr(img_data, "image_order", 0),
                    video_url=getattr(img_data, "video_url", None),
                ))

    # SEO: shared logic
    if updates.seo:
        if not product.seo: product.seo = ProductSEO(product_id=product.id)
        product.seo.page_title = updates.seo.page_title
        product.seo.meta_description = updates.seo.meta_description
        product.seo.meta_keywords = updates.seo.meta_keywords
        product.seo.url_handle = updates.seo.url_handle
        product.seo.canonical_url = updates.seo.canonical_url

    # Activity Log
    await log_activity(
        db,
        entity_type="product",
        entity_id=product.id,
        action="update",
        details={"sku": product.sku, "title": product.title},
        performed_by=user.get("admin_id"),
    )

    await db.commit()

    # --- Cache Invalidation ---
    # try:
    #     redis_client = redis.from_url(get_redis_url())
    #     await redis_client.delete(f"product_detail:{original_id}:True")
    #     await redis_client.delete(f"product_detail:{original_id}:False")
    #     if original_id != product_id:
    #         await redis_client.delete(f"product_detail:{product_id}:True")
    #         await redis_client.delete(f"product_detail:{product_id}:False")
    # except Exception as e:
    #     logger.error(f"Cache invalidation error: {e}")

    # Refresh and return
    refreshed = await db.execute(
        select(Product)
        .options(
            selectinload(Product.variants).selectinload(ProductVariant.attributes),
            selectinload(Product.variants).selectinload(ProductVariant.images),
            selectinload(Product.images),
            selectinload(Product.brand),
            selectinload(Product.category),
        )
        .where(Product.id == product.id)
    )
    updated_product = refreshed.scalar_one()
    # updated_product.brand_name = updated_product.brand.name if updated_product.brand else None
    # updated_product.category_name = updated_product.category.name if updated_product.category else None
    return updated_product


# ---------------- DELETE PRODUCT ----------------
@router.delete(
    "/delete-product/{product_id}",
    status_code=status.HTTP_200_OK,
    responses={
        404: {"description": "Product not found"},
    },
)
async def delete_product(
    product_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    user: Annotated[dict, Depends(require_catalog_supervisor)],
):
    # 🔹 Fetch product with variants to ensure cascade delete (safely handles children)
    result = await db.execute(
        select(Product)
        .options(
            selectinload(Product.variants).selectinload(ProductVariant.attributes),
            selectinload(Product.images),
        )
        .where(Product.id == product_id)
    )
    product = result.scalar_one_or_none()

    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Product not found"
        )

    # 🔹 Delete product (SQLAlchemy will cascade to variants, attributes, images)
    # Activity Log (logged before delete to capture info if needed, though here we just need ID)
    await log_activity(
        db,
        entity_type="product",
        entity_id=product.id,
        action="delete",
        details={"sku": product.sku, "title": product.title},
        performed_by=user.get("admin_id"),
    )

    await db.delete(product)
    await db.commit()

    return {"status": "success", "message": "Product deleted successfully"}


# ---------------- ARCHIVE PRODUCT ----------------
@router.post(
    "/archive-products",
    responses={
        400: {"description": "No product IDs provided."},
    },
)
async def archive_products(
    request: ProductArchiveRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    product_ids = request.product_ids

    if not product_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="No product IDs provided."
        )

    result = await db.execute(select(Product).where(Product.id.in_(product_ids)))
    products = result.scalars().all()

    found_ids = {p.id for p in products}
    missing_ids = [pid for pid in product_ids if pid not in found_ids]

    for product in products:
        product.status = "Archived"

    await db.commit()

    response = {
        "archived_count": len(products),
        "archived_products": [p.id for p in products],
    }

    if missing_ids:
        response["not_found"] = [
            {"id": pid, "message": "Product not found"} for pid in missing_ids
        ]

    return response


# ---------------- UPLOAD PRODUCT IMAGES----------------


@router.post(
    "/upload-images",
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {"description": "Mapping error or invalid file type."},
        404: {"description": "Product not found."},
        500: {"description": "GCS upload failed."},
    },
)
async def upload_product_images(
    product_id: Annotated[str, Form(...)],
    files: Annotated[list[UploadFile], File(...)],
    is_main_flags: Annotated[list[bool], Form(...)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    if len(files) != len(is_main_flags):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Number of files must match number of is_main flags.",
        )

    # Check if product exists
    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Product with id '{product_id}' not found.",
        )

    uploaded_images = []

    for file, is_main in zip(files, is_main_flags):
        if not file.content_type.startswith("image/"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid file type: {file.content_type}. Only images are allowed.",
            )

        file_bytes = await file.read()
        extension = file.filename.split(".")[-1]
        blob_name = f"products_{uuid.uuid4()}.{extension}"

        try:
            image_url = upload_to_gcs(
                file_bytes=file_bytes,
                blob_name=blob_name,
                content_type=file.content_type,
            )

            product_image = ProductImage(
                product_id=product_id,
                image_url=image_url,
                is_main=is_main,
            )
            db.add(product_image)
            uploaded_images.append({"url": image_url, "is_main": is_main})

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to upload {file.filename}: {str(e)}",
            )

    await db.commit()

    return {
        "message": f"{len(uploaded_images)} image(s) uploaded successfully.",
        "product_id": product_id,
        "uploaded_images": uploaded_images,
    }


# ---------------- RECOMMENDATIONS PRODUCT ----------------
@router.get(
    "/recommendations",
    response_model=List[ProductResponse],
    responses={
        400: {"description": "Missing filter parameters."},
        404: {"description": "Product, Category, or Brand not found."},
    },
)
async def recommend_products(
    db: Annotated[AsyncSession, Depends(get_db)],
    product_id: Annotated[Optional[str], Query()] = None,
    category_name: Annotated[Optional[str], Query()] = None,
    brand_name: Annotated[Optional[str], Query()] = None,
    product_name: Annotated[Optional[str], Query()] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
):
    query_filters = []
    base_category_id = None
    base_brand_id = None
    base_title = None

    # --- If product_id is given, use its category and brand ---
    if product_id:
        result = await db.execute(
            select(Product)
            .options(selectinload(Product.category), selectinload(Product.brand))
            .where(Product.id == product_id)
        )
        product = result.scalar_one_or_none()
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")

        base_category_id = product.category_id
        base_brand_id = product.brand_id
        base_title = product.title

        query_filters.append(Product.category_id == base_category_id)
        query_filters.append(Product.brand_id == base_brand_id)

    # --- Filter by category name ---
    elif category_name:
        result = await db.execute(
            select(Category.id).where(Category.name.ilike(f"%{category_name}%"))
        )
        category = result.scalar_one_or_none()
        if category:
            query_filters.append(Product.category_id == category)
        else:
            raise HTTPException(status_code=404, detail="Category not found")

    # --- Filter by brand name ---
    if brand_name:
        result = await db.execute(
            select(Brand.id).where(Brand.name.ilike(f"%{brand_name}%"))
        )
        brand = result.scalar_one_or_none()
        if brand:
            query_filters.append(Product.brand_id == brand)
        else:
            raise HTTPException(status_code=404, detail="Brand not found")

    # --- Filter by product title ---
    if product_name:
        query_filters.append(Product.title.ilike(f"%{product_name}%"))

    if not query_filters:
        raise HTTPException(
            status_code=400,
            detail="Provide at least one of product_id, category_name, brand_name, or product_name",
        )

    # ✅ Eager-load all relationships used in ProductResponse
    query = (
        select(Product)
        .options(
            selectinload(Product.brand),
            selectinload(Product.category),
            selectinload(Product.review_stats),
            selectinload(Product.variants).selectinload(ProductVariant.attributes),
            selectinload(Product.images),
            selectinload(Product.reviews),
        )
        .where(or_(*query_filters))
        .limit(limit)
    )

    result = await db.execute(query)
    products = result.scalars().unique().all()

    # Exclude the base product itself
    if product_id:
        products = [p for p in products if p.id != product_id]

    if not products:
        raise HTTPException(status_code=404, detail="No similar products found")

    return products


# ---------------- RECENTLY VIEWED ----------------
@router.get(
    "/recently-viewed",
    response_model=List[ProductResponse],
    responses={
        400: {"description": "No session or user found"},
    },
)
async def get_recently_viewed(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
):
    """Fetch recently viewed products (for logged-in or guest user)."""
    user_id = getattr(request.state, "user_id", None)
    session_id = request.cookies.get("session_id")
    logger.info(f"{user_id} {session_id} session_id")

    if not user_id and not session_id:
        raise HTTPException(status_code=400, detail="No session or user found")

    stmt = (
        select(Product)
        .join(RecentlyViewed, RecentlyViewed.product_id == Product.id)
        .where(RecentlyViewed.session_id == session_id)
        .order_by(RecentlyViewed.viewed_at.desc())
        .limit(limit)
        .options(
            selectinload(Product.brand),
            selectinload(Product.category),
            selectinload(Product.review_stats),
            selectinload(Product.reviews),
            selectinload(Product.variants).selectinload(ProductVariant.attributes),
            selectinload(Product.variants).selectinload(ProductVariant.images),
            selectinload(Product.images),
        )
    )

    result = await db.execute(stmt)
    products = result.scalars().unique().all()
    
    # Manual serialization to avoid recursion error in jsonable_encoder with SQLAlchemy objects
    return [ProductResponse.model_validate(p) for p in products]


# ---------------- POPULAR PRODUCTS ----------------


@router.get("/popular-products", response_model=dict)
async def get_popular_products(
    db: Annotated[AsyncSession, Depends(get_db)],
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1)] = 10,
    category_id: Annotated[
        Optional[str], Query(description="Filter by category (includes descendants)")
    ] = None,
    category_slug: Annotated[Optional[str], Query(description="Filter by category slug")] = None,
    brand_id: Annotated[Optional[str], Query(description="Filter by brand id")] = None,
):
    now = datetime.now()
    month_start = datetime(now.year, now.month, 1)

    # resolve subtree (optional)
    cat_ids: list[str] | None = None
    if category_id:
        cat_ids = await get_all_subcategory_ids_cte(db, category_id)
        if not cat_ids:
            cat_ids = [category_id]  # fallback
    elif category_slug:
        cat_slug_list = [s.strip() for s in category_slug.split(",") if s.strip()]
        cat_ids_res = await db.execute(select(Category.id).where(Category.slug.in_(cat_slug_list)))
        found_cat_ids = cat_ids_res.scalars().all()
        if found_cat_ids:
            cat_ids = set()
            for cid in found_cat_ids:
                subtree = await get_all_subcategory_ids_cte(db, cid)
                cat_ids.update(subtree)
            cat_ids = list(cat_ids)

    base_query = (
        select(Product)
        .options(
            selectinload(Product.review_stats),
            selectinload(Product.variants).selectinload(ProductVariant.attributes),
            selectinload(Product.brand),
            selectinload(Product.category),
            selectinload(Product.reviews),
            selectinload(Product.images),
        )
        .join(ProductStats, Product.id == ProductStats.product_id)
        .where(ProductStats.month_start == month_start)
    )

    if cat_ids:
        base_query = base_query.where(Product.category_id.in_(cat_ids))
    if brand_id:
        base_query = base_query.where(Product.brand_id == brand_id)

    count_query = (
        select(func.count(Product.id))
        .join(ProductStats, Product.id == ProductStats.product_id)
        .where(ProductStats.month_start == month_start)
    )

    if cat_ids:
        count_query = count_query.where(Product.category_id.in_(cat_ids))
    if brand_id:
        count_query = count_query.where(Product.brand_id == brand_id)

    total = (await db.execute(count_query)).scalar() or 0

    result = await db.execute(
        base_query.order_by(
            (
                ProductStats.views
                + ProductStats.orders * 2
                + ProductStats.added_to_cart
            ).desc()
        )
        .offset((page - 1) * limit)
        .limit(limit)
    )

    products = result.scalars().unique().all()

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "pages": (total + limit - 1) // limit,
        "items": [ProductResponse.model_validate(p) for p in products],
    }


# ---------------- TRENDING PRODUCTS ----------------
@router.get("/trending-products", response_model=dict)
async def get_trending_products(
    db: Annotated[AsyncSession, Depends(get_db)],
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1)] = 10,
):
    now = datetime.now()
    # Get start and end of the current day
    day_start = datetime(now.year, now.month, now.day)
    day_end = day_start + timedelta(days=1)

    # Base query with relationships
    base_query = (
        select(Product)
        .options(
            selectinload(Product.review_stats),
            selectinload(Product.variants).selectinload(ProductVariant.attributes),
            selectinload(Product.brand),
            selectinload(Product.category),
            selectinload(Product.reviews),
            selectinload(Product.images),
        )
        .join(ProductStats, Product.id == ProductStats.product_id)
        .where(ProductStats.created_at >= day_start)
        .where(ProductStats.created_at < day_end)  # 👈 restrict to today's data
    )

    # Count total records
    count_query = (
        select(func.count(Product.id))
        .join(ProductStats, Product.id == ProductStats.product_id)
        .where(ProductStats.created_at >= day_start)
        .where(ProductStats.created_at < day_end)
    )
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Apply ordering and pagination
    result = await db.execute(
        base_query.order_by(
            (
                ProductStats.views
                + ProductStats.orders * 3
                + ProductStats.added_to_cart * 2
            ).desc()
        )
        .offset((page - 1) * limit)
        .limit(limit)
    )

    products = result.scalars().unique().all()

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "pages": (total + limit - 1) // limit,
        "items": [ProductResponse.model_validate(p) for p in products],
    }

@router.get("/personalized", response_model=List[ProductResponse])
async def get_personalized_products(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
):
    """
    Fetch personalized product recommendations based on recently viewed products.
    """
    user_id = getattr(request.state, "user_id", None)
    session_id = request.cookies.get("session_id")

    if not session_id:
        trending = await get_trending_products(page=1, limit=limit, db=db)
        return trending["items"]

    # 1. Get recently viewed
    recent_stmt = (
        select(RecentlyViewed.product_id)
        .where(RecentlyViewed.session_id == session_id)
        .order_by(RecentlyViewed.viewed_at.desc())
        .limit(5)
    )
    result = await db.execute(recent_stmt)
    viewed_product_ids = result.scalars().all()

    if not viewed_product_ids:
        trending = await get_trending_products(page=1, limit=limit, db=db)
        return trending["items"]

    # 2. Get details of viewed products to find categories/brands
    details_stmt = select(Product.category_id, Product.brand_id).where(Product.id.in_(viewed_product_ids))
    result = await db.execute(details_stmt)
    details = result.all()
    
    category_ids = {row.category_id for row in details if row.category_id}
    brand_ids = {row.brand_id for row in details if row.brand_id}

    if not category_ids and not brand_ids:
         trending = await get_trending_products(page=1, limit=limit, db=db)
         return trending["items"]

    # 3. Find recommendations
    query = (
        select(Product)
        .where(
            or_(
                Product.category_id.in_(category_ids),
                Product.brand_id.in_(brand_ids)
            )
        )
        .where(Product.id.notin_(viewed_product_ids))
        .where(Product.status == "active")
        .order_by(Product.created_at.desc()) 
        .limit(limit)
        .options(
            selectinload(Product.brand),
            selectinload(Product.category),
            selectinload(Product.review_stats),
            selectinload(Product.reviews),
            selectinload(Product.variants).selectinload(ProductVariant.attributes),
            selectinload(Product.variants).selectinload(ProductVariant.images),
            selectinload(Product.images),
        )
    )

    result = await db.execute(query)
    recommendations = result.scalars().unique().all()
    
    if not recommendations:
         trending = await get_trending_products(page=1, limit=limit, db=db)
         return trending["items"]

    return recommendations


# ---------------- HOME SECTIONS----------------
@router.get(
    "/home-sections",
    responses={
        400: {"description": "Invalid section type"},
    },
)
async def get_home_section(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    section_type: Annotated[str, Query(description="Type of product section to fetch.")],
):
    valid_types = {
        "top_categories",
        "bestsellers",
        "top_rated",
        "recommended",
        "personalized",
        "customer_reviews",
    }

    if section_type not in valid_types:
        raise HTTPException(
            status_code=400, detail=f"Invalid section type: {section_type}"
        )

    # ===========================
    # 1️⃣ TOP CATEGORIES
    # ===========================
    if section_type == "top_categories":
        result = await db.execute(
            text(
                """
                SELECT 
                    c.id, 
                    c.name,
                    c.image_url,
                    c.icon_url,
                    COUNT(p.id) AS product_count
                FROM categories c
                LEFT JOIN products p ON p.category_id = c.id
                GROUP BY c.id
                ORDER BY product_count DESC
                LIMIT 10
            """
            )
        )
        return {"type": section_type, "data": result.mappings().all()}

    # ===========================
    # 2️⃣ BESTSELLERS
    # ===========================
    if section_type == "bestsellers":
        query = text(
            """
            WITH best_products AS (
                SELECT 
                    p.id,
                    p.title,
                    p.price,
                    p.rrp_price,
                    p.fast_dispatch,
                    p.free_shipping,
                    COUNT(r.id) AS review_count,
                    COALESCE(AVG(r.rating), 0) AS avg_rating,
                    MAX(pi.image_url) AS main_image
                FROM products p
                LEFT JOIN reviews r 
                    ON p.id = r.product_id
                LEFT JOIN product_images pi 
                    ON pi.product_id = p.id AND pi.is_main = 1
                GROUP BY p.id
                ORDER BY review_count DESC, avg_rating DESC
                LIMIT 10
            ),

            single_variant AS (
                SELECT 
                    pv.product_id,
                    pv.id AS variant_id,
                    pv.sku AS variant_sku,
                    pv.price AS variant_price,
                    pv.rrp_price AS variant_rrp_price,
                    pv.stock AS variant_stock,
                    COALESCE(pvi.image_url, pv.image_url) AS variant_image
                FROM product_variants pv
                LEFT JOIN product_variant_images pvi
                    ON pvi.variant_id = pv.id AND pvi.is_main = 1
                WHERE pv.id IN (
                    SELECT MIN(id)
                    FROM product_variants
                    GROUP BY product_id
                )
            )

            SELECT 
                bp.*,
                sv.variant_id,
                sv.variant_sku,
                sv.variant_price,
                sv.variant_rrp_price,
                sv.variant_stock,
                sv.variant_image
            FROM best_products bp
            LEFT JOIN single_variant sv
                ON sv.product_id = bp.id;
            """
        )

        result = await db.execute(query)
        data = result.mappings().all()
        return {"type": section_type, "data": data}

    # ===========================
    # 3️⃣ TOP RATED
    # ===========================
    if section_type == "top_rated":
        query = text(
            """
            WITH top_products AS (
                SELECT 
                    p.id,
                    p.title,
                    p.price,
                    p.rrp_price,
                    p.stock,
                    p.fast_dispatch,
                    p.free_shipping,
                    COUNT(r.id) AS review_count,
                    AVG(r.rating) AS avg_rating,
                    MAX(pi.image_url) AS main_image
                FROM reviews r
                JOIN products p 
                    ON p.id = r.product_id
                LEFT JOIN product_images pi
                    ON pi.product_id = p.id AND pi.is_main = 1
                GROUP BY p.id
                HAVING review_count >= 3
                ORDER BY avg_rating DESC, review_count DESC
                LIMIT 10
            ),

            single_variant AS (
                SELECT 
                    pv.product_id,
                    pv.id AS variant_id,
                    pv.sku AS variant_sku,
                    pv.price AS variant_price,
                    pv.rrp_price AS variant_rrp_price,
                    pv.stock AS variant_stock,
                    COALESCE(pvi.image_url, pv.image_url) AS variant_image
                FROM product_variants pv
                LEFT JOIN product_variant_images pvi
                    ON pvi.variant_id = pv.id AND pvi.is_main = 1
                WHERE pv.id IN (
                    SELECT MIN(id)
                    FROM product_variants
                    GROUP BY product_id
                )
            )

            SELECT 
                tp.*,
                sv.variant_id,
                sv.variant_sku,
                sv.variant_price,
                sv.variant_rrp_price,
                sv.variant_stock,
                sv.variant_image
            FROM top_products tp
            LEFT JOIN single_variant sv
                ON sv.product_id = tp.id;
            """
        )

        result = await db.execute(query)
        data = result.mappings().all()
        return {"type": section_type, "data": data}

    # ===========================
    # 4️⃣ RECOMMENDED (Generic)
    # ===========================
    if section_type == "recommended":
        query = text(
            """
            WITH recommended_products AS (
                SELECT 
                    p.id,
                    p.title,
                    p.price,
                    p.rrp_price,
                    p.stock,
                    p.fast_dispatch,
                    p.free_shipping,
                    COUNT(r.id) AS review_count,
                    COALESCE(AVG(r.rating), 0) AS average_rating,
                    MAX(pi.image_url) AS main_image
                FROM products p
                LEFT JOIN reviews r 
                    ON p.id = r.product_id
                LEFT JOIN product_images pi 
                    ON pi.product_id = p.id AND pi.is_main = 1
                GROUP BY p.id
                ORDER BY average_rating DESC, review_count DESC, p.created_at DESC
                LIMIT 10
            ),

            single_variant AS (
                SELECT 
                    pv.product_id,
                    pv.id AS variant_id,
                    pv.sku AS variant_sku,
                    pv.price AS variant_price,
                    pv.rrp_price AS variant_rrp_price,
                    pv.stock AS variant_stock,
                    COALESCE(pvi.image_url, pv.image_url) AS variant_image
                FROM product_variants pv
                LEFT JOIN product_variant_images pvi
                    ON pvi.variant_id = pv.id AND pvi.is_main = 1
                WHERE pv.id IN (
                    SELECT MIN(id)
                    FROM product_variants
                    GROUP BY product_id
                )
            )

            SELECT 
                rp.*,
                sv.variant_id,
                sv.variant_sku,
                sv.variant_price,
                sv.variant_rrp_price,
                sv.variant_stock,
                sv.variant_image
            FROM recommended_products rp
            LEFT JOIN single_variant sv
                ON sv.product_id = rp.id;
            """
        )

        result = await db.execute(query)
        data = result.mappings().all()
        return {"type": section_type, "data": data}

    # ===========================
    # 5️⃣ PERSONALIZED (Dummy -> Future)
    # ===========================
    # ===========================
    # 5️⃣ PERSONALIZED
    # ===========================
    if section_type == "personalized":
        # Call the new endpoint logic
        items = await get_personalized_products(request=request, db=db, limit=10)
        # Serialize since get_personalized_products returns ORM objects
        data = [jsonable_encoder(item) for item in items]
        return {"type": section_type, "data": data}

    # ===========================
    # 6️⃣ CUSTOMER REVIEWS (Most Reviewed)
    # ===========================
    if section_type == "customer_reviews":
        query = text(
            """
            SELECT 
                p.id AS product_id,
                p.title AS product_title,
                r.reviewer_name,
                r.rating,
                r.comment AS review_text,
                r.created_at AS review_date
            FROM reviews r
            JOIN products p ON p.id = r.product_id
            ORDER BY r.created_at DESC
            LIMIT 15
        """
        )

        result = await db.execute(query)
        data = result.mappings().all()
        return {"type": section_type, "data": data}



@router.post(
    "/upload-any-image",
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {"description": "Invalid upload type, video format, or corrupted image."},
        500: {"description": "Upload failed."},
    },
)
async def upload_any_image(
    upload_type: Annotated[str, Form(description="product | category | brand | extras")],
    file: Annotated[UploadFile, File(...)],
    video: Annotated[str, Form(description="yes | no")] = "no",
):

    allowed_types: Set[str] = {"product", "category", "brand", "extras"}
    if upload_type not in allowed_types:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid upload_type. Allowed: {', '.join(allowed_types)}",
        )

    is_video = video.lower() == "yes"

    if is_video:
        # Validate extension
        extension = file.filename.split(".")[-1].lower()
        if extension != "mp4":
            raise HTTPException(
                status_code=400,
                detail="Only MP4 videos are allowed.",
            )

        # Validate MIME type
        if file.content_type != "video/mp4":
            raise HTTPException(
                status_code=400,
                detail="Invalid video type. Only video/mp4 allowed.",
            )

        file_bytes = await file.read()

        # Clean filename
        name, _ = os.path.splitext(file.filename)
        name = re.sub(r"[^a-zA-Z0-9_-]", "_", name)

        cleaned_filename = f"{name}.mp4"
        unique_id = str(uuid.uuid4())
        blob_name = f"{upload_type}/videos/{unique_id}_{cleaned_filename}"

        try:
            video_url = upload_to_gcs(
                file_bytes=file_bytes,
                blob_name=blob_name,
                content_type="video/mp4",
            )

            return {
                "message": "Video uploaded successfully.",
                "upload_type": upload_type,
                "media_type": "video",
                "video_url": video_url,
            }

        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Video upload failed: {str(e)}",
            )

    allowed_extensions = {"jpg", "jpeg", "png"}
    extension = file.filename.split(".")[-1].lower()

    if extension not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail="Only JPG, JPEG, and PNG files are allowed.",
        )

    if file.content_type not in {"image/jpeg", "image/png"}:
        raise HTTPException(
            status_code=400,
            detail="Invalid image type. Only JPG and PNG allowed.",
        )

    original_bytes = await file.read()

    try:
        image = Image.open(BytesIO(original_bytes))

        if image.mode in ("RGBA", "P"):
            image = image.convert("RGB")

        webp_buffer = BytesIO()
        image.save(
            webp_buffer,
            format="WEBP",
            quality=80,
            method=6,
        )

        file_bytes = webp_buffer.getvalue()

    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Invalid or corrupted image file.",
        )

    # Clean filename
    name, _ = os.path.splitext(file.filename)
    name = re.sub(r"[^a-zA-Z0-9_-]", "_", name)

    cleaned_filename = f"{name}.webp"
    unique_id = str(uuid.uuid4())
    blob_name = f"{upload_type}/images/{unique_id}_{cleaned_filename}"

    try:
        image_url = upload_to_gcs(
            file_bytes=file_bytes,
            blob_name=blob_name,
            content_type="image/webp",
        )

        return {
            "message": "Image uploaded successfully.",
            "upload_type": upload_type,
            "media_type": "image",
            "image_url": image_url,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Image upload failed: {str(e)}",
        )


@router.patch(
    "/status/{product_id}",
    response_model=ProductStatusOut,
    responses={
        404: {"description": "Product not found"},
    },
)
async def update_product_status(
    product_id: str, data: ProductStatusUpdate, db: Annotated[AsyncSession, Depends(get_db)]
):
    # Fetch product
    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()

    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    # Update status
    product.status = data.status
    product.updated_at = func.now()

    await db.commit()
    await db.refresh(product)

    return product


# -----------------------------PARTIAL PRODUCT UPDATE-----------------------------


@router.patch(
    "/partial-update/{product_id}",
    response_model=ProductResponseWithImages,
    responses={
        400: {"description": "Invalid update_type or variant mapping error."},
        404: {"description": "Product not found"},
    },
)
async def partial_update_product(
    product_id: str,
    updates: ProductUpdateWithImages,  # SAME JSON AS FULL UPDATE
    db: Annotated[AsyncSession, Depends(get_db)],
    update_type: Annotated[str, Query(description="comma separated: price,stock,images")],
    variant_id: Annotated[
        str | None,
        Query(description="If provided, update BOTH product and this specific variant"),
    ] = None,
):
    # -------------------------------------
    # Parse update_type
    # -------------------------------------
    allowed = {t.strip().lower() for t in update_type.split(",") if t.strip()}
    valid = {"price", "stock", "images"}

    if not allowed.issubset(valid):
        raise HTTPException(
            400, detail=f"Invalid update_type. Allowed: {', '.join(valid)}"
        )

    update_price = "price" in allowed
    update_stock = "stock" in allowed
    update_images = "images" in allowed

    # -------------------------------------
    # Load product
    # -------------------------------------
    result = await db.execute(
        select(Product)
        .options(
            selectinload(Product.variants).selectinload(ProductVariant.attributes),
            selectinload(Product.variants).selectinload(ProductVariant.images),
            selectinload(Product.images),
            selectinload(Product.brand),
            selectinload(Product.category),
        )
        .where(Product.id == product_id)
    )
    product = result.scalar_one_or_none()
    if not product:
        raise HTTPException(404, "Product not found")

    # -------------------------------------
    # PRODUCT-LEVEL UPDATES
    # (ALWAYS APPLY, WHETHER VARIANT_ID IS PASSED OR NOT)
    # -------------------------------------

    # price + cost_price
    if update_price:
        if updates.price is not None:
            product.price = updates.price
        if updates.cost_price is not None:
            product.cost_price = updates.cost_price

    # stock
    if update_stock and updates.stock is not None:
        product.stock = updates.stock

    # images
    if update_images and updates.images is not None:
        await db.execute(
            ProductImage.__table__.delete().where(ProductImage.product_id == product.id)
        )

        first_image = None
        for img in updates.images:
            db.add(
                ProductImage(
                    product_id=product.id,
                    image_url=img.image_url,
                    is_main=img.is_main,
                )
            )
            if first_image is None:
                first_image = img.image_url

    # -------------------------------------
    # VARIANT-LEVEL UPDATE (ONLY IF VARIANT_ID IS PROVIDED)
    # -------------------------------------
    if variant_id:
        # fetch & validate variant
        v_result = await db.execute(
            select(ProductVariant).where(
                ProductVariant.id == variant_id,
                ProductVariant.product_id == product.id,
            )
        )
        variant = v_result.scalar_one_or_none()
        if not variant:
            raise HTTPException(400, f"Variant {variant_id} does not belong to product")

        # find variant block from request body
        v_src = None
        if updates.variants:
            for v in updates.variants:
                if v.id == variant_id:
                    v_src = v
                    break

        # fallback
        if not v_src:
            v_src = updates

        # update variant price + cost_price
        if update_price:
            if getattr(v_src, "price", None) is not None:
                variant.price = v_src.price
            if getattr(v_src, "cost_price", None) is not None:
                variant.cost_price = v_src.cost_price

        # update variant stock
        if update_stock:
            if getattr(v_src, "stock", None) is not None:
                variant.stock = v_src.stock

        # update variant images
        if update_images:
            if getattr(v_src, "images", None) is not None:
                await db.execute(
                    ProductVariantImage.__table__.delete().where(
                        ProductVariantImage.variant_id == variant.id
                    )
                )

                first_vimg = None
                for img in v_src.images:
                    db.add(
                        ProductVariantImage(
                            variant_id=variant.id,
                            image_url=img.image_url,
                            is_main=img.is_main,
                        )
                    )
                    if first_vimg is None:
                        first_vimg = img.image_url

                if first_vimg:
                    variant.image_url = first_vimg

    # -------------------------------------
    # Commit & Reload
    # -------------------------------------
    await db.commit()

    refreshed = await db.execute(
        select(Product)
        .options(
            selectinload(Product.variants).selectinload(ProductVariant.attributes),
            selectinload(Product.variants).selectinload(ProductVariant.images),
            selectinload(Product.images),
            selectinload(Product.brand),
            selectinload(Product.category),
        )
        .where(Product.id == product.id)
    )
    updated_product = refreshed.scalar_one()

    # updated_product.brand_name = (
    #     updated_product.brand.name if updated_product.brand else None
    # )
    # updated_product.category_name = (
    #     updated_product.category.name if updated_product.category else None
    # )

    return updated_product


# TODO Shopify and Big Commerce importing
# Woocommerce
# Update any product that already exist in db
# Bundle/Combo indicator


@router.post("/generate-missing-codes")
async def generate_missing_codes(db: Annotated[AsyncSession, Depends(get_db)]):
    """
    Generate unique_code for all products that have it as NULL.
    """
    # 1. Fetch products with NULL unique_code
    result = await db.execute(select(Product).where(Product.unique_code == None))
    products = result.scalars().all()

    updated_count = 0
    for product in products:
        # 2. Generate and assign code
        product.unique_code = await generate_unique_product_code(db)
        updated_count += 1

    # 3. Commit changes
    if updated_count > 0:
        await db.commit()

    return {"message": f"Successfully updated {updated_count} products with unique codes."}


@router.delete("/delete-all-products", status_code=status.HTTP_204_NO_CONTENT)
async def delete_all_products(db: Annotated[AsyncSession, Depends(get_db)]):
    """
    Delete ALL products and related data from the database.
    WARNING: This is a destructive action.
    """
    try:
        logger.info("Deleting all products...")
        # if not allow_delete:
        #     raise HTTPException(
        #         status_code=status.HTTP_400_BAD_REQUEST,
        #         detail="Delete not allowed.",
        #     )
        # Delete related data first to avoid FK constraints
        await db.execute(delete(ProductSEO))
        await db.execute(delete(ProductStock))
        await db.execute(delete(Review))
        await db.execute(delete(ReviewStats))
        await db.execute(delete(ProductStats))
        await db.execute(delete(RecentlyViewed))
        await db.execute(delete(ProductHighlightItem))
        await db.execute(delete(StockReservation))
        
        await db.execute(delete(ProductVariantImage))
        await db.execute(delete(ProductImage))
        await db.execute(delete(Attribute))
        await db.execute(delete(ProductVariant))
        await db.execute(delete(Product))
        
        await db.commit()
        return None
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete all products: {str(e)}"
        )
