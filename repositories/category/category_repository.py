from sqlalchemy import select, func, text, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from typing import List, Optional, Set, Dict
from models.category import Category, CategoryAttribute, CategoryAttributeValue
from models.product import Product
from repositories.base import BaseRepository
from apis.v1.utils import make_slug

class CategoryRepository(BaseRepository[Category]):
    def __init__(self, db: AsyncSession):
        super().__init__(db, Category)

    async def generate_unique_slug(self, name: str, category_id: Optional[str] = None) -> str:
        base_slug = make_slug(name)
        slug = base_slug
        counter = 1
        while True:
            stmt = select(Category).where(Category.slug == slug)
            if category_id:
                stmt = stmt.where(Category.id != category_id)
            result = await self.db.execute(stmt)
            if not result.scalar_one_or_none():
                break
            slug = f"{base_slug}-{counter}"
            counter += 1
        return slug

    async def get_with_details(self, category_id: str) -> Optional[Category]:
        result = await self.db.execute(
            select(Category)
            .options(
                selectinload(Category.subcategories)
                .selectinload(Category.subcategories)
                .selectinload(Category.subcategories)
                .selectinload(Category.subcategories),
                selectinload(Category.categories_attributes).selectinload(
                    CategoryAttribute.values
                ),
            )
            .where(Category.id == category_id)
        )
        category = result.scalar_one_or_none()
        if category:
            category.attributes = category.categories_attributes
        return category

    async def get_all_subcategory_ids_cte(self, category_id: str) -> List[str]:
        sql = text(
            """
            WITH RECURSIVE subcategories AS (
                SELECT id, parent_id
                FROM categories
                WHERE id = :category_id
                UNION ALL
                SELECT c.id, c.parent_id
                FROM categories c
                INNER JOIN subcategories sc ON sc.id = c.parent_id
            )
            SELECT id FROM subcategories;
            """
        )
        result = await self.db.execute(sql, {"category_id": category_id})
        return [row[0] for row in result.fetchall()]

    async def get_active_category_ids_cte(self) -> Set[str]:
        sql = text(
            """
            WITH RECURSIVE active_tree AS (
                SELECT id, parent_id
                FROM categories
                WHERE is_active = true
                  AND parent_id IS NULL
                UNION ALL
                SELECT c.id, c.parent_id
                FROM categories c
                JOIN active_tree at ON at.id = c.parent_id
                WHERE c.is_active = true
            )
            SELECT id FROM active_tree;
            """
        )
        result = await self.db.execute(sql)
        return {row[0] for row in result.fetchall()}

    async def get_product_counts_for_all(self) -> Dict[str, int]:
        result = await self.db.execute(
            select(Product.category_id, func.count(Product.id)).group_by(
                Product.category_id
            )
        )
        return {row[0]: row[1] for row in result.fetchall()}

    async def get_all_flat(self, is_admin: bool = False, q: Optional[str] = None) -> List[Category]:
        query = select(Category)
        if not is_admin:
            query = query.where(Category.is_active == True)
        if q:
            query = query.where(Category.name.ilike(f"%{q}%"))
        result = await self.db.execute(query)
        return result.scalars().all()

    async def get_active_subtree_ids_cte(self, category_id: str) -> List[str]:
        sql = text(
            """
            WITH RECURSIVE active_subcategories AS (
                SELECT id, parent_id, is_active
                FROM categories
                WHERE id = :category_id AND is_active = true
                UNION ALL
                SELECT c.id, c.parent_id, c.is_active
                FROM categories c
                INNER JOIN active_subcategories sc ON sc.id = c.parent_id
                WHERE c.is_active = true
            )
            SELECT id FROM active_subcategories;
            """
        )
        result = await self.db.execute(sql, {"category_id": category_id})
        return [row[0] for row in result.fetchall()]

    async def delete_all(self):
        await self.db.execute(text("UPDATE categories SET parent_id = NULL"))
        await self.db.execute(text("DELETE FROM categories"))
