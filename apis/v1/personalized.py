"""
Personalized Product Recommendations API
=========================================
Supports both logged-in users (X-User-Id header) and anonymous guests (session_id cookie).

Endpoints:
  POST /track-view          — Record a product view (increments stats + recently_viewed)
  GET  /personalized        — Get personalized product recommendations
  GET  /home-sections       — Home section wrapper (section_type=personalized)
"""

from fastapi import APIRouter, Depends, Query, Request, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, desc, update, and_, or_
from sqlalchemy.orm import selectinload
from typing import Optional, List, Annotated
from decimal import Decimal
from datetime import datetime, timedelta
import uuid

from deps import get_db
from models.product import Product, RecentlyViewed, SearchHistory, ProductStats, ProductImage
from models.review import ReviewStats
from models.brand import Brand
from models.category import Category
from pydantic import BaseModel
from utils.promotions_client import fetch_applicable_promotions, calculate_best_promotion
from utils.promotions_utils import calculate_discounted_price

router = APIRouter()


# ─────────────────────────────────────────────
# Pydantic Schemas
# ─────────────────────────────────────────────

class TrackViewRequest(BaseModel):
    product_id: str
    session_id: str
    user_id: Optional[str] = None  # optional for logged-in users


class PersonalizedProductOut(BaseModel):
    id: str
    title: str
    slug: str
    unique_code: Optional[str] = None
    price: Optional[Decimal] = None
    rrp_price: Optional[Decimal] = None
    thumbnail: Optional[str] = None   # first product image url
    category_id: str
    brand_id: str
    brand_name: Optional[str] = None
    category_name: Optional[str] = None
    average_rating: Optional[float] = None
    total_reviews: Optional[int] = None
    status: str
    promotion_name: Optional[str] = None
    discount_percentage: float = 0
    discounted_price: float = 0

    class Config:
        from_attributes = True

class PaginatedPersonalizedProductResponse(BaseModel):
    page: int
    limit: int
    total: int
    pages: int
    data: List[PersonalizedProductOut]
    filters: Optional[list] = []


# ─────────────────────────────────────────────
# Helper: resolve identity (user_id or session_id)
# ─────────────────────────────────────────────

def _resolve_identity(request: Request, user_id_header: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (user_id, session_id).
    Priority: X-User-Id header > query param > cookie session_id.
    """
    user_id = user_id_header or request.headers.get("X-User-Id")
    session_id = request.cookies.get("session_id")
    return user_id, session_id


# ─────────────────────────────────────────────
# Helper: fetch recently viewed product IDs
# ─────────────────────────────────────────────

async def _get_recently_viewed_ids(
    db: AsyncSession,
    user_id: Optional[str],
    session_id: Optional[str],
    days: int = 30,
    limit: int = 50,
) -> List[str]:
    """Return list of product_ids the user/session has recently viewed."""
    if not user_id and not session_id:
        return []

    cutoff = datetime.utcnow() - timedelta(days=days)
    filters = [RecentlyViewed.viewed_at >= cutoff]

    if user_id:
        filters.append(RecentlyViewed.user_id == user_id)
    elif session_id:
        filters.append(RecentlyViewed.session_id == session_id)

    result = await db.execute(
        select(RecentlyViewed.product_id)
        .where(*filters)
        .order_by(RecentlyViewed.viewed_at.desc())
        .limit(limit)
    )
    return [row[0] for row in result.all()]


# ─────────────────────────────────────────────
# Helper: build lightweight product response
# ─────────────────────────────────────────────

def _build_product_out(product: Product, review_stats: Optional[ReviewStats], promo=None) -> PersonalizedProductOut:
    thumbnail = None
    if product.images:
        # prefer is_main image, else first
        main = next((img for img in product.images if img.is_main), None)
        thumbnail = (main or product.images[0]).image_url

    promotion_name = None
    discount_percentage = 0.0
    discounted_price = 0.0

    if promo:
        promotion_name = promo.offer_name
        discount_percentage = promo.discount_percentage
        discounted_price = calculate_discounted_price(
            product.price, promo.discount_percentage, promo.max_discount_amount
        )

    return PersonalizedProductOut(
        id=product.id,
        title=product.title,
        slug=product.slug,
        unique_code=product.unique_code,
        price=product.price,
        rrp_price=product.rrp_price,
        thumbnail=thumbnail,
        category_id=product.category_id,
        brand_id=product.brand_id,
        brand_name=product.brand.name if product.brand else None,
        category_name=product.category.name if product.category else None,
        average_rating=review_stats.average_rating if review_stats else None,
        total_reviews=review_stats.total_reviews if review_stats else None,
        status=product.status or "active",
        promotion_name=promotion_name,
        discount_percentage=discount_percentage,
        discounted_price=discounted_price,
    )


# ─────────────────────────────────────────────
# POST /track-view
# ─────────────────────────────────────────────

@router.post(
    "/track-view",
    status_code=status.HTTP_200_OK,
    responses={
        404: {"description": "Product not found"},
    },
)
async def track_product_view(
    payload: TrackViewRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Record a product view for a user or session.
    - Upserts into recently_viewed (deduplicates within 1 hour)
    - Increments ProductStats.views counter
    """
    # 1. Verify product exists
    result = await db.execute(select(Product.id).where(Product.id == payload.product_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Product not found")

    # 2. Upsert recently_viewed (avoid duplicate within 1 hour)
    one_hour_ago = datetime.utcnow() - timedelta(hours=1)
    filters = [
        RecentlyViewed.product_id == payload.product_id,
        RecentlyViewed.viewed_at >= one_hour_ago,
    ]
    if payload.user_id:
        filters.append(RecentlyViewed.user_id == payload.user_id)
    else:
        filters.append(RecentlyViewed.session_id == payload.session_id)

    existing = await db.execute(select(RecentlyViewed).where(*filters))
    existing_view = existing.scalar_one_or_none()

    if existing_view:
        # Refresh timestamp
        existing_view.viewed_at = func.now()
    else:
        new_view = RecentlyViewed(
            product_id=payload.product_id,
            session_id=payload.session_id,
            user_id=payload.user_id,
        )
        db.add(new_view)

    # 3. Increment ProductStats.views (upsert)
    stats_result = await db.execute(
        select(ProductStats).where(
            ProductStats.product_id == payload.product_id,
            ProductStats.week_start == None,
            ProductStats.month_start == None,
        )
    )
    stats = stats_result.scalar_one_or_none()
    if stats:
        stats.views = (stats.views or 0) + 1
        stats.updated_at = func.now()
    else:
        db.add(ProductStats(
            product_id=payload.product_id,
            views=1,
            orders=0,
            added_to_cart=0,
        ))

    await db.commit()
    return {"detail": "View tracked"}


# ─────────────────────────────────────────────
# GET /personalized
# ─────────────────────────────────────────────

@router.get("/personalized", response_model=PaginatedPersonalizedProductResponse)
async def get_personalized_products(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    user_id: Annotated[Optional[str], Query(description="Logged-in user ID")] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
):
    """
    Returns personalized product recommendations.

    Logic:
    1. Identify user via X-User-Id header or session_id cookie.
    2. Fetch recently viewed products → extract category/brand affinity.
    3. Query active products matching those categories/brands (excluding viewed ones).
    4. Score by review rating + view popularity.
    5. Fallback to trending if < 10 personalized results.
    """
    resolved_user_id, session_id = _resolve_identity(request, user_id)

    # ── Step 1: Get viewed product IDs ──
    viewed_ids = await _get_recently_viewed_ids(db, resolved_user_id, session_id)

    personalized_products: List[Product] = []
    category_ids: List[str] = []
    brand_ids: List[str] = []

    if viewed_ids:
        # ── Step 2: Extract category/brand affinity from viewed products ──
        affinity_result = await db.execute(
            select(Product.category_id, Product.brand_id)
            .where(Product.id.in_(viewed_ids))
        )
        for row in affinity_result.all():
            if row[0] and row[0] not in category_ids:
                category_ids.append(row[0])
            if row[1] and row[1] not in brand_ids:
                brand_ids.append(row[1])

    if category_ids or brand_ids:
        # ── Step 3: Query matching products (excluding already viewed) ──
        affinity_filter = or_(
            Product.category_id.in_(category_ids) if category_ids else False,
            Product.brand_id.in_(brand_ids) if brand_ids else False,
        )

        stmt = (
            select(Product)
            .where(
                Product.status == "active",
                Product.id.notin_(viewed_ids),
                affinity_filter,
            )
            .options(
                selectinload(Product.images),
                selectinload(Product.brand),
                selectinload(Product.category),
                selectinload(Product.review_stats),
            )
            # ── Step 4: Score — join stats for ordering ──
            .outerjoin(ProductStats, and_(
                ProductStats.product_id == Product.id,
                ProductStats.week_start == None,
                ProductStats.month_start == None,
            ))
            .outerjoin(ReviewStats, ReviewStats.product_id == Product.id)
            .order_by(
                desc(func.coalesce(ReviewStats.average_rating, 0) * 0.4 +
                     func.coalesce(ProductStats.views, 0) * 0.4 +
                     func.coalesce(ProductStats.orders, 0) * 0.2)
            )
            .limit(limit)
        )

        result = await db.execute(stmt)
        personalized_products = result.scalars().unique().all()

    # ── Step 5: Trending fallback ──
    if len(personalized_products) < 10:
        needed = limit - len(personalized_products)
        existing_ids = [p.id for p in personalized_products] + viewed_ids

        trending_stmt = (
            select(Product)
            .where(
                Product.status == "active",
                Product.id.notin_(existing_ids) if existing_ids else True,
            )
            .options(
                selectinload(Product.images),
                selectinload(Product.brand),
                selectinload(Product.category),
                selectinload(Product.review_stats),
            )
            .outerjoin(ProductStats, and_(
                ProductStats.product_id == Product.id,
                ProductStats.week_start == None,
                ProductStats.month_start == None,
            ))
            .outerjoin(ReviewStats, ReviewStats.product_id == Product.id)
            .order_by(
                desc(func.coalesce(ProductStats.views, 0) +
                     func.coalesce(ProductStats.orders, 0))
            )
            .limit(needed)
        )
        trending_result = await db.execute(trending_stmt)
        trending_products = trending_result.scalars().unique().all()
        personalized_products = list(personalized_products) + list(trending_products)

    # ── Fetch Promos ──
    promotions = await fetch_applicable_promotions()

    # ── Build response ──
    items = []
    for p in personalized_products:
        promo = calculate_best_promotion(str(p.id), str(p.category_id), promotions)
        items.append(_build_product_out(p, p.review_stats, promo))
    
    # We apply manual slice here to support pagination over the combined trending + personalized results
    total_items = len(items)
    pages = (total_items + limit - 1) // limit if limit > 0 else 1
    offset = (page - 1) * limit
    paginated_items = items[offset:offset+limit]

    return {
        "page": page,
        "limit": limit,
        "total": total_items, # Note: this is total items found this run, not absolute total in DB
        "pages": pages,
        "data": paginated_items,
        "filters": []
    }


# ─────────────────────────────────────────────
# GET /home-sections
# ─────────────────────────────────────────────

@router.get(
    "/home-sections",
    responses={
        400: {"description": "Unsupported section_type"},
    },
)
async def get_home_section(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    section_type: Annotated[str, Query(description="Section type, e.g. 'personalized'")],
    user_id: Annotated[Optional[str], Query()] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
):
    """
    Generic home section endpoint.
    When section_type=personalized, returns personalized product recommendations.
    """
    if section_type == "personalized":
        products_response = await get_personalized_products(
            request=request,
            user_id=user_id,
            page=page,
            limit=limit,
            db=db,
        )
        return {
            "type": "personalized",
            "title": "Recommended For You",
            "data": products_response["data"], # Unwrap to list or keep nested? Usually sections have data as list
            "page": products_response["page"],
            "limit": products_response["limit"],
            "total": products_response["total"],
            "pages": products_response["pages"]
        }

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Unsupported section_type '{section_type}'. Supported: personalized",
    )
