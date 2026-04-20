from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from deps import get_db
from schemas.featured_brand import FeaturedBrandCreate, FeaturedBrandUpdate, FeaturedBrandResponse
from service.featured_brand import FeaturedBrandService
from typing import Annotated, List

router = APIRouter(tags=["Featured Brands"])

NOT_FOUND_MSG = "Featured Brand not found"


@router.post("/", response_model=FeaturedBrandResponse)
async def create_featured_brand(
    featured_brand: FeaturedBrandCreate,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    return await FeaturedBrandService.create_featured_brand(db, featured_brand)


@router.get("/", response_model=List[FeaturedBrandResponse])
async def get_featured_brands(
    db: Annotated[AsyncSession, Depends(get_db)],
    skip: int = 0,
    limit: int = 100,
):
    return await FeaturedBrandService.get_featured_brands(db, skip, limit)


@router.get(
    "/{featured_brand_id}",
    response_model=FeaturedBrandResponse,
    responses={
        404: {"description": "Featured Brand not found."},
    },
)
async def get_featured_brand(
    featured_brand_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    db_featured_brand = await FeaturedBrandService.get_featured_brand(db, featured_brand_id)
    if db_featured_brand is None:
        raise HTTPException(status_code=404, detail=NOT_FOUND_MSG)
    return db_featured_brand


@router.put(
    "/{featured_brand_id}",
    response_model=FeaturedBrandResponse,
    responses={
        404: {"description": "Featured Brand not found."},
    },
)
async def update_featured_brand(
    featured_brand_id: str,
    featured_brand: FeaturedBrandUpdate,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    db_featured_brand = await FeaturedBrandService.update_featured_brand(db, featured_brand_id, featured_brand)
    if db_featured_brand is None:
        raise HTTPException(status_code=404, detail=NOT_FOUND_MSG)
    return db_featured_brand


@router.delete(
    "/{featured_brand_id}",
    responses={
        404: {"description": "Featured Brand not found."},
    },
)
async def delete_featured_brand(
    featured_brand_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    success = await FeaturedBrandService.delete_featured_brand(db, featured_brand_id)
    if not success:
        raise HTTPException(status_code=404, detail=NOT_FOUND_MSG)
    return {"detail": "Featured Brand deleted"}
