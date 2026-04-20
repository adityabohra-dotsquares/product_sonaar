# apis/v1/promotion.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from models.promotions import Promotion
from schemas.promotions import PromotionCreate, PromotionResponse
from deps import get_db
from utils.promotions_utils import validate_reference_exists
from datetime import datetime
from typing import Annotated

router = APIRouter()


@router.get(
    "/get-promotion/{product_id}",
    responses={
        404: {"description": "No active promotions found"},
    },
)
async def get_promotions_by_product(
    product_id: str, db: Annotated[AsyncSession, Depends(get_db)]
):
    result = await db.execute(
        select(Promotion).where(
            Promotion.reference_id == product_id, Promotion.status == "active"
        )
    )
    promotions = result.scalars().all()

    if not promotions:
        raise HTTPException(status_code=404, detail="No active promotions found")

    return [
        {
            "id": promo.id,
            "offer_name": promo.offer_name,
            "discount_type": promo.discount_type,
            "discount_value": promo.discount_value,
            "discount_percentage": promo.discount_percentage,
            "description": promo.description,
            "start_date": promo.start_date,
            "end_date": promo.end_date,
        }
        for promo in promotions
    ]


@router.post(
    "/create-promotions",
    response_model=PromotionResponse,
    responses={
        400: {"description": "An active promotion already exists for this entity."},
    },
)
async def create_promotion(
    data: PromotionCreate, db: Annotated[AsyncSession, Depends(get_db)]
):
    """Create a new promotional offer for product or category with reference validation."""

    await validate_reference_exists(db, data.offer_type, data.reference_id)

    existing = await db.execute(
        select(Promotion)
        .where(Promotion.offer_type == data.offer_type)
        .where(Promotion.reference_id == data.reference_id)
        .where(Promotion.status == "active")
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"An active promotion already exists for this {data.offer_type}.",
        )

    new_promo = Promotion(
        offer_name=data.name,
        description=data.description,
        discount_type=data.discount_type,
        discount_value=data.discount_value,
        discount_percentage=data.discount_percentage,
        offer_type=data.offer_type,
        reference_id=data.reference_id,
        start_date=data.start_date,
        end_date=data.end_date,
    )

    db.add(new_promo)
    await db.commit()
    await db.refresh(new_promo)

    return new_promo


@router.get("/list-promotions", response_model=list[PromotionResponse])
async def list_promotions(db: Annotated[AsyncSession, Depends(get_db)]):
    """List all promotions with updated status."""
    result = await db.execute(select(Promotion))
    promotions = result.scalars().all()

    # Update status dynamically using current_status property
    for promo in promotions:
        promo.status = promo.current_status

    return promotions


@router.delete(
    "/delete-promotions/{promotion_id}",
    responses={
        404: {"description": "Promotion not found"},
    },
)
async def delete_promotion(
    promotion_id: str, db: Annotated[AsyncSession, Depends(get_db)]
):
    """Delete a promotion by ID."""
    result = await db.execute(select(Promotion).where(Promotion.id == promotion_id))
    promo = result.scalar_one_or_none()

    if not promo:
        raise HTTPException(status_code=404, detail="Promotion not found")

    await db.delete(promo)
    await db.commit()
    return {"message": "Promotion deleted successfully"}
