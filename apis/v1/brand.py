from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from deps import get_db
from models.brand import Brand
from schemas.brand import BrandCreate, BrandUpdate, BrandOut
from fastapi import status
from fastapi import Query
import pandas as pd
from fastapi import UploadFile, File
from schemas.brand import BrandStatusUpdate, BrandOutStatus
from sqlalchemy import func
from typing import Annotated, Optional, List, Generic, TypeVar, Literal
from sqlalchemy import func, case
from openpyxl import Workbook
from openpyxl.styles import Font
from fastapi.responses import StreamingResponse
import csv
import io
from models.product import Product
from utils.activity_logger import log_activity
from apis.v1.utils import make_slug
import utils.constants as constants 
router = APIRouter()

from pydantic import BaseModel
from pydantic import BaseModel

T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    page: int
    limit: int
    total: int
    pages: int
    data: List[T]


@router.get("/list-brands", response_model=PaginatedResponse[BrandOut])
async def get_brands(
    db: Annotated[AsyncSession, Depends(get_db)],
    page: Annotated[int, Query(ge=1)] = 1,
    size: Annotated[int, Query(ge=1, le=1000)] = 1000,
    status: Annotated[str, Query(pattern="^(active|inactive|all)$")] = "active",
    sort_by: Annotated[str, Query(pattern="^(name|created_at)$")] = "name",
    sort_dir: Annotated[str, Query(pattern="^(asc|desc)$")] = "asc",
    download: Annotated[str, Query(pattern="^(yes|no)$")] = "no",
    q: Annotated[Optional[str], Query(description="Search brand by name")] = None,
    only_with_products: Annotated[bool, Query(description="Filter brands with at least one product")] = True,
):
    offset = (page - 1) * size
    print(size,page)

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
            select(Product.id)
            .where(Product.brand_id == Brand.id)
            .exists()
        )
    total = await db.scalar(total_query)

    # Pagination
    result = await db.execute(query.offset(offset).limit(size))
    rows = result.mappings().all()

    # CSV download
    if download == "yes":
        output = io.StringIO()
        writer = csv.writer(output)

        writer.writerow(
            [
                "Brand ID",
                "Brand Name",
                "Total Products",
                "Active Products",
                "Inactive Products",
            ]
        )

        for r in rows:
            writer.writerow(
                [
                    r["id"],
                    r["name"],
                    r["total_products"],
                    r["active_products"],
                    r["inactive_products"],
                ]
            )

        output.seek(0)
        return StreamingResponse(
            output,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=brand_report.csv"},
        )

    return PaginatedResponse(
        total=total,
        page=page,
        limit=size,
        pages=(total + size - 1) // size,
        data=rows,
    )


# Get a brand by ID
@router.get(
    "/get-brand/{brand_id}",
    response_model=BrandOut,
    responses={
        404: {"description": "Brand not found."},
    },
)
async def get_brand(brand_id: str, db: Annotated[AsyncSession, Depends(get_db)]):
    result = await db.execute(select(Brand).where(Brand.id == brand_id))
    brand = result.scalar_one_or_none()
    if not brand:
        raise HTTPException(status_code=404, detail=constants.messages.get("brand_not_found", "Brand not found"))
    return brand


@router.get(
    "/get-brand-by-slug/{slug}",
    response_model=BrandOut,
    responses={
        404: {"description": "Brand not found."},
    },
)
async def get_brand_by_slug(slug: str, db: Annotated[AsyncSession, Depends(get_db)]):
    result = await db.execute(select(Brand).where(Brand.slug == slug))
    brand = result.scalar_one_or_none()
    if not brand:
        raise HTTPException(status_code=404, detail=constants.messages.get("brand_not_found", "Brand not found"))
    return brand


@router.patch(
    "/status/{brand_id}/",
    response_model=BrandOutStatus,
    responses={
        404: {"description": "Brand not found."},
    },
)
async def update_brand_status(
    brand_id: str, data: BrandStatusUpdate, db: Annotated[AsyncSession, Depends(get_db)]
):
    # Fetch brand
    result = await db.execute(select(Brand).where(Brand.id == brand_id))
    brand = result.scalar_one_or_none()

    if not brand:
        raise HTTPException(status_code=404, detail="Brand not found")

    # Update status
    brand.is_active = data.is_active
    brand.updated_at = func.now()

    await db.commit()
    await db.refresh(brand)

    return brand


@router.get("/search")
async def search_brands(
    db: Annotated[AsyncSession, Depends(get_db)],
    q: Annotated[Optional[str], Query(description="Search brand by name")] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
):
    offset = (page - 1) * limit

    # ---- BASE QUERY (Only Active Brands) ----
    query = select(
        Brand.id,
        Brand.name,
        Brand.slug,
        Brand.logo_url,
        Brand.is_active,
        Brand.created_at,
        Brand.updated_at,
    ).where(Brand.is_active == True)

    # ---- APPLY SEARCH ----
    if q:
        search_term = f"%{q.strip()}%"
        query = query.where(Brand.name.ilike(search_term))

    # ---- COUNT ----
    total_query = select(func.count()).select_from(query.subquery())
    total = (await db.execute(total_query)).scalar_one()

    # ---- PAGINATION ----
    result = await db.execute(
        query.order_by(Brand.name.asc()).limit(limit).offset(offset)
    )

    rows = result.mappings().all()

    return {
        "query": q,
        "page": page,
        "limit": limit,
        "total": total,
        "pages": (total + limit - 1) // limit,
        "count": len(rows),
        "results": rows,
    }

