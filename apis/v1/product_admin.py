from typing import Optional, List, Annotated
from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import select, func, or_
from sqlalchemy.ext.asyncio import AsyncSession
from schemas.product import ProductResponse
from deps import get_db
from models.product import *
from fastapi import HTTPException
from sqlalchemy.orm import selectinload
from models.brand import Brand
from models.warehouse import ProductStock
from service.product import get_fuzzy_matched_ids
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
from apis.v1.utils import generate_unique_product_code
from apis.v1.import_export import make_slug
from service.warehouse import validate_warehouse, set_stock
from utils.activity_logger import log_activity
from utils.admin_auth import require_catalog_supervisor
from utils.constants import messages
from utils.image_handler import download_and_upload_image
router = APIRouter()

from pydantic import BaseModel, Field
from typing import Optional
from service import product
from schemas.product import ProductFilter


# ── Validate Identifiers ──────────────────────────────────────────────────────

class ValidateIdentifiersRequest(BaseModel):
    identifiers: List[str]


@router.post("/validate-identifiers")
async def validate_identifiers(
    payload: ValidateIdentifiersRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Validate a list of product identifiers (UUIDs or SKUs).
    Checks both Product and ProductVariant tables.
    Returns per-identifier result with found status, id, sku, and title.
    """
    results = []
    for raw in payload.identifiers:
        identifier = raw.strip()
        if not identifier:
            continue

        found_id    = None
        found_sku   = None
        found_title = None
        found       = False

        # 1. Try ProductVariant (by id or sku)
        variant_result = await db.execute(
            select(ProductVariant, Product.title.label("parent_title"))
            .join(Product, ProductVariant.product_id == Product.id)
            .where(
                or_(
                    ProductVariant.id  == identifier,
                    func.lower(ProductVariant.sku) == identifier.lower(),
                )
            )
            .limit(1)
        )
        row = variant_result.first()
        if row:
            variant, parent_title = row
            found_id    = variant.id
            found_sku   = variant.sku
            found_title = variant.title or parent_title
            found       = True
        else:
            # 2. Try parent Product (by id or sku)
            product_result = await db.execute(
                select(Product)
                .where(
                    or_(
                        Product.id  == identifier,
                        func.lower(Product.sku) == identifier.lower(),
                    )
                )
                .limit(1)
            )
            product = product_result.scalar_one_or_none()
            if product:
                found_id    = product.id
                found_sku   = product.sku
                found_title = product.title
                found       = True

        results.append({
            "identifier": identifier,
            "id":         found_id,
            "sku":        found_sku,
            "title":      found_title,
            "found":      found,
        })
    return {"results": results}


@router.get("/get-products")
async def get_products(
    filters: Annotated[ProductFilter, Depends()],
    db: Annotated[AsyncSession, Depends(get_db)],
    user: Annotated[dict, Depends(require_catalog_supervisor)],
):
    return await product.get_products(db, filters, user)

@router.get(
    "/get-product/{product_id}",
    responses={
        404: {"description": "Product or variant not found"},
    },
)
async def get_product(
    product_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    user: Annotated[dict, Depends(require_catalog_supervisor)],
):
    product_id = product_id.strip()
    
    # Try to find a variant first if it's a specific variant match
    variant_query = (
        select(Product, ProductVariant)
        .join(ProductVariant, Product.id == ProductVariant.product_id)
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
                ProductVariant.sku == product_id
            )
        )
    )
    
    variant_result = await db.execute(variant_query)
    match = variant_result.first()
    
    if match:
        product, variant = match
        return {
            "id": variant.id,
            "product_id": product.id,
            "sku": variant.sku,
            "title": variant.title,
            "description": variant.description or product.description,
            "slug": variant.slug or product.slug,
            "price": variant.price,
            "cost_price": variant.cost_price if variant.cost_price is not None else product.cost_price,
            "rrp_price": variant.rrp_price if variant.rrp_price is not None else product.rrp_price,
            "stock": variant.stock,
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
            "images": variant.images if variant.images else product.images,
            "variants": [],
            "is_variant": True,
            "created_at": variant.created_at,
            "updated_at": variant.updated_at,
            "added_by": variant.added_by,
            "updated_by": variant.updated_by,
            "vendor_id": product.vendor_id,
        }

    # If no variant match, try parent product
    product_query = (
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
                Product.sku == product_id
            )
        )
    )
    
    product_result = await db.execute(product_query)
    product = product_result.scalar_one_or_none()

    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Product or variant not found"
        )
        
    return product



# @router.get("/get-product/{product_id}")
# async def get_product(
#     product_id: str,
#     db: AsyncSession = Depends(get_db),
# ):
#     return await product.get_product(db, product_id)


@router.post(
    "/create-product",
    # response_model=ProductResponseWithVariantImages,
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
    print("CREATING PRODUCT",product)
    # check product SKU
    existing_product = await db.execute(
        select(Product).where(Product.sku == product.sku)
    )
    user_created_by = user.get("user_id")
    print("USER CREATED BY",user_created_by)
    if existing_product.scalar_one_or_none():
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
        price=product.price,
        cost_price=product.cost_price,
        status=product.status,
        weight=product.weight,
        tags=product.tags,
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
        added_by=user_created_by,
        vendor_id=user.get("vendor_id"),
        # ean=product.product_code,
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
            added_by=user["user_id"],
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
        performed_by="admin"  # TODO: get actual user if available
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
