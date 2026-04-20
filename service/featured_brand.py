from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from models.featured_brand import FeaturedBrand
from schemas.featured_brand import FeaturedBrandCreate, FeaturedBrandUpdate

class FeaturedBrandService:
    @staticmethod
    async def create_featured_brand(db: AsyncSession, featured_brand: FeaturedBrandCreate):
        db_featured_brand = FeaturedBrand(**featured_brand.dict())
        db.add(db_featured_brand)
        await db.commit()
        await db.refresh(db_featured_brand)
        return db_featured_brand

    @staticmethod
    async def get_featured_brands(db: AsyncSession, skip: int = 0, limit: int = 100):
        result = await db.execute(
            select(FeaturedBrand).order_by(FeaturedBrand.position).offset(skip).limit(limit)
        )
        return result.scalars().all()

    @staticmethod
    async def get_featured_brand(db: AsyncSession, featured_brand_id: str):
        result = await db.execute(
            select(FeaturedBrand).where(FeaturedBrand.id == featured_brand_id)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def update_featured_brand(db: AsyncSession, featured_brand_id: str, featured_brand: FeaturedBrandUpdate):
        db_featured_brand = await FeaturedBrandService.get_featured_brand(db, featured_brand_id)
        if db_featured_brand:
            for key, value in featured_brand.dict(exclude_unset=True).items():
                setattr(db_featured_brand, key, value)
            await db.commit()
            await db.refresh(db_featured_brand)
        return db_featured_brand

    @staticmethod
    async def delete_featured_brand(db: AsyncSession, featured_brand_id: str):
        db_featured_brand = await FeaturedBrandService.get_featured_brand(db, featured_brand_id)
        if db_featured_brand:
            await db.delete(db_featured_brand)
            await db.commit()
            return True
        return False
