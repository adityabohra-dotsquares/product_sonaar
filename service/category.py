from fastapi.requests import Request
from fastapi.responses import JSONResponse, Response
from fastapi import Depends, HTTPException, status
from models.category import Category, CategoryAttribute, CategoryAttributeValue
from models.product import Product
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
from sqlalchemy.exc import IntegrityError
from collections import defaultdict
from typing import List, Set
from repositories.category.category_repository import CategoryRepository
from utils.activity_logger import log_activity
from utils.image_handler import download_and_upload_image
from utils import constants


async def generate_unique_category_slug(
    db: AsyncSession, name: str, category_id: str | None = None
) -> str:
    repo = CategoryRepository(db)
    return await repo.generate_unique_slug(name, category_id)


# Create
async def create_category(
    db: AsyncSession,
    name: str,
    parent_id: str | None = None,
    image_url: str | None = None,
    icon_url: str | None = None,
    attributes: list | None = None,
    category_code: str | None = None,
    slug: str | None = None,
):
    repo = CategoryRepository(db)

    # Check if category already exists
    existing = await repo.db.execute(
        select(Category).where(Category.name == name, Category.parent_id == parent_id)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=constants.messages.get(
                "category_already_exists_under_parent",
                "Category {name} already exists under this parent.",
            ).format(name=name),
        )

    # Check if category_code already exists
    if category_code:
        code_existing = await repo.db.execute(
            select(Category).where(Category.category_code == category_code)
        )
        if code_existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=constants.messages.get(
                    "category_code_already_exists",
                    "Category code {category_code} already exists.",
                ).format(category_code=category_code),
            )

    # 1️⃣ Create Category
    if not slug:
        slug = await repo.generate_unique_slug(name)

    # Process images
    if image_url:
        image_url = await download_and_upload_image(image_url, identifier="category")
    if icon_url:
        icon_url = await download_and_upload_image(icon_url, identifier="category")

    category = Category(
        name=name,
        slug=slug,
        parent_id=parent_id,
        image_url=str(image_url) if image_url else None,
        icon_url=str(icon_url) if icon_url else None,
        category_code=category_code,
    )

    repo.add(category)
    await repo.flush()

    # 2️⃣ Add Attributes and Values
    if attributes:
        for attr in attributes:
            attr_obj = CategoryAttribute(
                name=attr.name,
                category_id=category.id,
                is_active=attr.is_active,
            )
            db.add(attr_obj)
            await db.flush()

            if attr.values:
                for val in attr.values:
                    val_obj = CategoryAttributeValue(
                        value=val.value,
                        is_active=val.is_active,
                        attribute_id=attr_obj.id,
                    )
                    db.add(val_obj)

    # 3️⃣ Commit transaction
    try:
        await log_activity(
            db,
            entity_type="category",
            entity_id=category.id,
            action="create",
            details={"name": category.name},
            performed_by="admin",
        )
        await db.commit()
    except IntegrityError as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

    return await repo.get_with_details(category.id)


async def build_category_tree(db: AsyncSession, parent_id=None):

    repo = CategoryRepository(db)
    result = await db.execute(select(Category).where(Category.parent_id == parent_id))
    categories = result.scalars().all()

    tree = []
    for cat in categories:
        # Get all subcategory IDs to count products correctly
        sub_ids = await repo.get_all_subcategory_ids_cte(cat.id)
        from models.product import Product

        prod_count_res = await db.execute(
            select(func.count(Product.id)).where(Product.category_id.in_(sub_ids))
        )
        product_count = prod_count_res.scalar() or 0

        sub_tree = await build_category_tree(db, cat.id)
        tree.append(
            {
                "id": cat.id,
                "name": cat.name,
                "parent_id": cat.parent_id,
                "image_url": cat.image_url,
                "icon_url": cat.icon_url,
                "product_count": product_count,
                "subcategories": sub_tree,
            }
        )
    return tree


async def get_all_categories(db: AsyncSession):
    return await build_category_tree(db)


async def get_all_categories_flat(
    db: AsyncSession, is_admin: bool = False, q: str = None
):
    repo = CategoryRepository(db)
    return await repo.get_all_flat(is_admin, q)


async def get_all_product_counts(db: AsyncSession, is_admin: bool = False):
    repo = CategoryRepository(db)

    if is_admin:
        result = await db.execute(
            select(Product.category_id, func.count(Product.id))
            .join(Category, Category.id == Product.category_id)
            .group_by(Product.category_id)
        )
    else:
        result = await db.execute(
            select(Product.category_id, func.count(Product.id))
            .join(Category, Category.id == Product.category_id)
            .where(Category.is_active == True)
            .group_by(Product.category_id)
        )
    return {row[0]: row[1] for row in result.fetchall()}


def build_category_tree_fast(
    categories,
    product_counts,
    only_with_products: bool = False,
    only_active: bool = False,
):
    by_parent = defaultdict(list)
    category_ids = {cat.id for cat in categories}
    for cat in categories:
        by_parent[cat.parent_id].append(cat)

    cache = {}

    def get_total_count(cat_id):
        if cat_id in cache:
            return cache[cat_id]
        total = product_counts.get(cat_id, 0)
        for sub in by_parent.get(cat_id, []):
            total += get_total_count(sub.id)
        cache[cat_id] = total
        return total

    def build_branch(nodes):
        res = []
        for cat in nodes:
            if only_active and not cat.is_active:
                continue
            if only_with_products and get_total_count(cat.id) == 0:
                continue
            res.append(
                {
                    "id": cat.id,
                    "category_code": cat.category_code,
                    "slug": cat.slug,
                    "name": cat.name,
                    "parent_id": cat.parent_id,
                    "image_url": cat.image_url,
                    "icon_url": cat.icon_url,
                    "is_active": cat.is_active,
                    "product_count": get_total_count(cat.id),
                    "subcategories": build_branch(by_parent.get(cat.id, [])),
                }
            )
        return res

    roots = [cat for cat in categories if cat.parent_id not in category_ids]
    return build_branch(roots)


async def get_category(db: AsyncSession, category_id: str):
    repo = CategoryRepository(db)
    return await repo.get_with_details(category_id)


async def update_category(
    db: AsyncSession,
    category_id: str,
    name=None,
    parent_id=None,
    image_url=None,
    icon_url=None,
    category_code=None,
    slug=None,
):
    repo = CategoryRepository(db)
    category = await repo.get_with_details(category_id)
    if not category:
        return None

    if name:
        category.name = name
        if not slug:
            category.slug = await repo.generate_unique_slug(name, category_id)
    if slug:
        category.slug = slug
    if parent_id is not None:
        category.parent_id = parent_id
    if image_url:
        category.image_url = await download_and_upload_image(
            image_url, identifier="category"
        )
    if icon_url:
        category.icon_url = await download_and_upload_image(
            icon_url, identifier="category"
        )
    if category_code:
        category.category_code = category_code

    await log_activity(
        db,
        entity_type="category",
        entity_id=category.id,
        action="update",
        details={"name": category.name},
        performed_by="admin",
    )
    await db.commit()
    await db.refresh(category)
    return category


async def delete_category(db: AsyncSession, category_id: str):
    repo = CategoryRepository(db)
    category = await repo.get_by_id(category_id)
    if not category:
        return None

    child_count_res = await db.execute(
        select(func.count(Category.id)).where(Category.parent_id == category_id)
    )
    if child_count_res.scalar() > 0:
        raise HTTPException(
            status_code=400,
            detail=constants.messages.get(
                "category_delete_failed_leaf_exist",
                "Cannot delete non-leaf category. Delete subcategories first.",
            ),
        )

    await repo.delete(category)
    await log_activity(
        db,
        entity_type="category",
        entity_id=category_id,
        action="delete",
        details={"name": category.name},
        performed_by="admin",
    )
    await db.commit()
    return category


async def delete_all_categories(db: AsyncSession):
    repo = CategoryRepository(db)

    prod_count = (await db.execute(select(func.count(Product.id)))).scalar()
    if prod_count > 0:
        raise HTTPException(
            status_code=400, detail="Cannot delete categories with active products."
        )

    await repo.delete_all()
    await log_activity(
        db,
        entity_type="category",
        entity_id="all",
        action="delete_all",
        details={},
        performed_by="admin",
    )
    await db.commit()
    return True


async def get_all_subcategory_ids_cte(db: AsyncSession, category_id: str) -> List[str]:
    repo = CategoryRepository(db)
    return await repo.get_all_subcategory_ids_cte(category_id)


async def get_active_category_ids_cte(db: AsyncSession) -> Set[str]:
    repo = CategoryRepository(db)
    return await repo.get_active_category_ids_cte()


async def get_active_subtree_ids(db: AsyncSession, category_id: str) -> List[str]:
    repo = CategoryRepository(db)
    return await repo.get_active_subtree_ids_cte(category_id)


async def get_active_category_hierarchy(db: AsyncSession):
    categories = await get_all_categories_flat(db, is_admin=False)
    product_counts = await get_all_product_counts(db, is_admin=False)
    return build_category_tree_fast(categories, product_counts, only_with_products=True, only_active=True)


async def compute_recursive_counts(db: AsyncSession, categories: List[Category]):
    counts = await get_all_product_counts(db, is_admin=True)
    
    by_parent = defaultdict(list)
    category_map = {}
    for cat in categories:
        by_parent[cat.parent_id].append(cat)
        category_map[cat.id] = cat
        
    recursive_counts = {}
    def get_total_count(cat_id):
        if cat_id in recursive_counts:
            return recursive_counts[cat_id]
        total = counts.get(cat_id, 0)
        for sub in by_parent.get(cat_id, []):
            total += get_total_count(sub.id)
        recursive_counts[cat_id] = total
        return total
        
    for cat in categories:
        get_total_count(cat.id)
        
    return recursive_counts, category_map


def serialize_category(category, counts, category_map):
    base = {
        "id": category.id,
        "name": category.name,
        "slug": category.slug,
        "category_code": category.category_code,
        "parent_id": category.parent_id,
        "image_url": category.image_url,
        "icon_url": category.icon_url,
        "is_active": category.is_active,
        "product_count": counts.get(category.id, 0),
        "subcategories": []
    }
    if hasattr(category, "subcategories_list"):
        base["subcategories"] = [
            serialize_category(sub, counts, category_map) 
            for sub in category.subcategories_list
        ]
    return base
