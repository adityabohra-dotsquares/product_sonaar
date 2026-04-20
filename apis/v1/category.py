from fastapi import APIRouter, status, Query, UploadFile
from fastapi.requests import Request
from fastapi.responses import JSONResponse, Response
from fastapi import Depends, HTTPException
import database
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from utils.promotions_utils import get_applicable_promotion, calculate_discounted_price
from pydantic import BaseModel
from typing import List, Generic, TypeVar
from schemas.category import CategoryWithProducts
from models.product import Product, ProductVariant
from pydantic import parse_obj_as
from io import BytesIO
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import aliased
from sqlalchemy import or_
from schemas.category import CategoryStatusUpdate, CategoryOutStatus
import uuid
from typing import Annotated, Optional, List, Dict, Any, Literal
from fastapi import Query
import json
import hashlib
from service.redis import get_redis_url, create_redis_client
import redis.asyncio as redis
from fastapi.encoders import jsonable_encoder
from openpyxl import Workbook
from openpyxl.styles import Font
import io
import csv
from fastapi.responses import StreamingResponse

router = APIRouter()

T = TypeVar("T")
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from models.category import Category
from service.category import *
from schemas.category import *
from sqlalchemy.orm import Session
from deps import get_db
from models.activity_log import ActivityLog
from utils.image_handler import download_and_upload_image


class PaginatedResponse(BaseModel, Generic[T]):
    total: int
    page: int
    size: int
    items: List[T]


@router.get("/list-category")
async def get_category_hierarchy(
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Returns the full category hierarchy for customer-facing use.

    Rules:
    - Only **active** categories are included (is_active=True).
    - If a parent category is inactive, neither it nor any of its children appear.
    - Categories (and their subtrees) with **zero products** are excluded.
    - The result is a nested tree (subcategories embedded inside each node).
    """
    return await get_active_category_hierarchy(db)


@router.get(
    "/get-category/{category_id}",
    responses={
        404: {"description": "Category not found."},
    },
)
async def get_category(category_id: str, db: Annotated[AsyncSession, Depends(get_db)]):
    # Step 1 — Load all categories
    result = await db.execute(select(Category))
    all_categories = result.scalars().all()

    # Step 2 — Compute product counts (recursive) and build category map
    counts, category_map = await compute_recursive_counts(db, all_categories)

    # Step 3 — Build parent-child tree
    for c in all_categories:
        if c.parent_id and c.parent_id in category_map:
            parent = category_map[c.parent_id]
            if not hasattr(parent, "subcategories_list"):
                parent.subcategories_list = []
            parent.subcategories_list.append(c)

    # Step 4 — Get requested category
    category = category_map.get(category_id)
    if not category:
        raise HTTPException(status_code=404, detail="Category not found")

    # Step 5 — Serialize with counts
    return serialize_category(category, counts, category_map)


@router.get(
    "/get-category-by-slug/{slug}",
    responses={
        404: {"description": "Category not found."},
    },
)
async def get_category_by_slug(slug: str, db: Annotated[AsyncSession, Depends(get_db)]):
    # Step 1 — Load all categories
    result = await db.execute(select(Category))
    all_categories = result.scalars().all()

    # Step 2 — Compute product counts (recursive) and build category map
    counts, category_map = await compute_recursive_counts(db, all_categories)

    # Step 3 — Build parent-child tree
    for c in all_categories:
        if c.parent_id and c.parent_id in category_map:
            parent = category_map[c.parent_id]
            if not hasattr(parent, "subcategories_list"):
                parent.subcategories_list = []
            parent.subcategories_list.append(c)

    # Step 4 — Get requested category by slug
    category = next((c for c in all_categories if c.slug == slug), None)
    if not category:
        raise HTTPException(status_code=404, detail="Category not found")

    # Step 5 — Serialize with counts
    return serialize_category(category, counts, category_map)


@router.put(
    "/update-category/{category_id}",
    response_model=CategoryRead,
    responses={
        404: {"description": "Category not found."},
    },
)
async def update_category_details(
    category_id: str, category: CategoryUpdate, db: Annotated[AsyncSession, Depends(get_db)]
):
    updated = await update_category(
        db,
        category_id,
        category.name,
        category.parent_id,
        category.image_url,
        icon_url=category.icon_url,
        category_code=category.category_code,
        slug=category.slug,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Category not found")
    return updated


@router.delete(
    "/delete-category/{category_id}",
    responses={
        404: {"description": "Category not found."},
    },
)
async def delete_category_by_id(category_id: str, db: Annotated[AsyncSession, Depends(get_db)]):
    print("Starting delete_category_by_id CUSTOM")
    deleted = await delete_category(db, category_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Category not found")
    return {"detail": "Category deleted successfully"}


@router.get(
    "/category-with-products/{category_id}",
    response_model=CategoryWithProducts,
    responses={
        404: {"description": "Category not found."},
    },
)
async def get_category_with_products(
    category_id: str, db: Annotated[AsyncSession, Depends(get_db)]
):
    """Get a category along with its products, variants, and promotion info."""
    result = await db.execute(
        select(Category)
        .options(
            selectinload(Category.products)
            .selectinload(Product.variants)
            .selectinload(ProductVariant.attributes)
        )
        .where(Category.id == category_id)
    )
    category = result.scalar_one_or_none()
    if not category:
        raise HTTPException(status_code=404, detail="Category not found")

    # Enrich products with promotion info
    enriched_products = []
    for product in category.products:
        promo = await get_applicable_promotion(db, product)
        product_dict = product.__dict__.copy()
        product_dict["unique_code"] = product.unique_code

        if promo:
            product_dict["promotion_name"] = promo.offer_name
            if promo.discount_type == "percentage":
                product_dict["discount_percentage"] = promo.discount_percentage or 0
                product_dict["discounted_price"] = calculate_discounted_price(
                    product.price, promo.discount_percentage or 0
                )
            else:
                product_dict["promotion_name"] = promo.offer_name
                product_dict["discount_percentage"] = 0
                product_dict["discounted_price"] = float(product.price) - float(
                    promo.discount_value
                )
        else:
            product_dict["promotion_name"] = None
            product_dict["discount_percentage"] = 0
            product_dict["discounted_price"] = float(product.price)

        enriched_products.append(product_dict)

    # Return category and enriched products using Pydantic
    return CategoryWithProducts(
        id=category.id,
        name=category.name,
        parent_id=category.parent_id,
        image_url=category.image_url,
        icon_url=category.icon_url,
        is_active=category.is_active,
        created_at=category.created_at,
        updated_at=category.updated_at,
        products=parse_obj_as(list[ProductResponse], enriched_products),
    )


@router.get(
    "/categories-with-attributes",
    response_model=SubCategoryFilterResponse,
    summary="Get subcategories with attributes and filters",
    responses={
        404: {"description": "Parent category not found."},
    },
)
async def get_subcategories(
    db: Annotated[AsyncSession, Depends(get_db)],
    parent_id: Annotated[str, Query(description="Parent category ID")] = ...,
    is_active: Annotated[bool | None, Query(description="Filter by active status")] = None,
    name: Annotated[str | None, Query(description="Filter by name (partial match)")] = None,
    attribute_name: Annotated[str | None, Query(description="Filter by attribute name")] = None,
):
    """
    Fetch subcategories of a given parent category with optional filters:
    - is_active
    - name (partial match)
    """

    # ✅ First, fetch parent category to include its name
    parent_category_result = await db.execute(
        select(Category).where(Category.id == parent_id)
    )
    parent_category = parent_category_result.scalar_one_or_none()

    if not parent_category:
        raise HTTPException(status_code=404, detail="Category not found")

    # ✅ Then, fetch subcategories with attributes
    query = (
        select(Category)
        .where(Category.parent_id == parent_id)
        .options(selectinload(Category.categories_attributes))
    )

    if is_active is not None:
        query = query.where(Category.is_active == is_active)

    if name:
        query = query.where(Category.name.ilike(f"%{name}%"))

    result = await db.execute(query)
    subcategories = result.scalars().unique().all()

    # ✅ Apply attribute filtering in Python
    filtered_subcategories = []
    for subcat in subcategories:
        attributes = subcat.categories_attributes
        if attribute_name:
            attributes = [
                attr
                for attr in attributes
                if attribute_name.lower() in attr.name.lower()
            ]

        subcat_data = SubCategoryResponse(
            id=subcat.id,
            name=subcat.name,
            is_active=subcat.is_active,
            created_at=subcat.created_at,
            updated_at=subcat.updated_at,
            added_by=subcat.added_by,
            updated_by=subcat.updated_by,
            attributes=attributes,
        )
        filtered_subcategories.append(subcat_data)

    return SubCategoryFilterResponse(
        parent_id=parent_id,
        category_name=parent_category.name,
        image_url=parent_category.image_url,
        icon_url=parent_category.icon_url,
        created_by=parent_category.added_by,
        updated_by=parent_category.updated_by,
        subcategories=filtered_subcategories,
    )


# --------------------------------------UPLOAD CATEGORIES USING EXCEL,CSV--------------------------------------
@router.post(
    "/upload-categories",
    responses={
        400: {"description": "Unsupported file format or invalid columns."},
    },
)
async def upload_categories(
    file: Annotated[UploadFile, None],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    if not file.filename.endswith((".xlsb", ".xlsx", ".csv")):
        raise HTTPException(status_code=400, detail="Unsupported file format")

    content = await file.read()
    if file.filename.endswith(".xlsb"):
        df = pd.read_excel(BytesIO(content), sheet_name="Main Sheet", engine="pyxlsb")
    elif file.filename.endswith(".csv"):
        df = pd.read_csv(BytesIO(content))
    else:
        df = pd.read_excel(BytesIO(content))

    # --- Step 2: Normalize and Validate Data ---
    df.columns = df.columns.str.strip().str.lower()
    # Replace NaN/NaT with None for DB compatibility
    df = df.where(pd.notnull(df), None)

    # Validate columns
    ALLOWED_COLUMNS = {
        "category code",
        "categories",
        "image url",
        "image_url",
        "icon url",
        "icon_url",
        "action",
    }
    invalid_cols = [col for col in df.columns if col not in ALLOWED_COLUMNS]
    if invalid_cols:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid columns found: {invalid_cols}. Allowed columns are: {sorted(list(ALLOWED_COLUMNS))}",
        )

    # Validate mandatory columns
    MANDATORY_COLUMNS = {"category code", "categories", "image url", "icon url"}
    missing_cols = [col for col in MANDATORY_COLUMNS if col not in df.columns]
    if missing_cols:
        raise HTTPException(
            status_code=400,
            detail=f"Missing mandatory columns: {missing_cols}. Please download the template for correct headers.",
        )

    # Identify columns
    code_col = next(
        (col for col in df.columns if col in ["category code", "code"]), "category code"
    )
    cat_col = next(
        (col for col in df.columns if col in ["categories", "category"]), "categories"
    )
    action_col = next((col for col in df.columns if col == "action"), None)
    image_col = next(
        (col for col in df.columns if col in ["image url", "image_url"]), None
    )
    icon_col = next(
        (col for col in df.columns if col in ["icon url", "icon_url"]), None
    )

    # Cleanup rows
    df = df.dropna(subset=[code_col, cat_col])

    # Load existing categories
    result = await db.execute(select(Category))
    categories_all = result.scalars().all()
    existing = {(c.name.strip(), c.parent_id): c for c in categories_all}

    created = 0
    updated = 0
    deleted = 0

    for _, row in df.iterrows():
        category_code = str(row[code_col]).strip()
        hierarchy = str(row[cat_col]).strip()
        action = (
            str(row[action_col]).strip().lower()
            if action_col and row[action_col]
            else ""
        )

        if not hierarchy or any(s in category_code.lower() for s in ["nan", "none"]):
            continue

        if action == "delete":
            try:
                stmt = select(Category).where(
                    or_(
                        Category.id == category_code,
                        Category.category_code == category_code,
                    )
                )
                res = await db.execute(stmt)
                cat_to_del = res.scalar_one_or_none()
                if cat_to_del:
                    await delete_category(db, cat_to_del.id)
                    deleted += 1
            except Exception:
                pass
            continue

        levels = [lvl.strip() for lvl in hierarchy.split(">") if lvl.strip()]
        parent_id = None
        last_category_id = None

        for idx, level_name in enumerate(levels):
            key = (level_name, parent_id)

            # Already exists
            if key in existing:
                cat_obj = existing[key]

                # Update values if it's the last level
                if idx == len(levels) - 1:
                    has_changes = False

                    if category_code and cat_obj.category_code != category_code:
                        cat_obj.category_code = category_code
                        has_changes = True

                    if image_col:
                        val = row.get(image_col)
                        img_url = (
                            val
                            if val and str(val).lower() not in ["nan", "none"]
                            else None
                        )
                        if img_url:
                            img_url = await download_and_upload_image(
                                img_url, identifier="category"
                            )

                        if cat_obj.image_url != img_url:
                            cat_obj.image_url = img_url
                            has_changes = True

                    if icon_col:
                        val = row.get(icon_col)
                        ico_url = (
                            val
                            if val and str(val).lower() not in ["nan", "none"]
                            else None
                        )
                        if ico_url:
                            ico_url = await download_and_upload_image(
                                ico_url, identifier="category"
                            )

                        if cat_obj.icon_url != ico_url:
                            cat_obj.icon_url = ico_url
                            has_changes = True

                    if has_changes:
                        updated += 1
                        await db.flush()

                last_category_id = cat_obj.id
                parent_id = last_category_id
                continue

            # Last level — use category code as ID and handle image/icon
            img_url = None
            ico_url = None
            if idx == len(levels) - 1:
                new_id = category_code
                code = category_code

                if image_col:
                    val = row.get(image_col)
                    img_url = (
                        val if val and str(val).lower() not in ["nan", "none"] else None
                    )
                    if img_url:
                        img_url = await download_and_upload_image(
                            img_url, identifier="category"
                        )

                if icon_col:
                    val = row.get(icon_col)
                    ico_url = (
                        val if val and str(val).lower() not in ["nan", "none"] else None
                    )
                    if ico_url:
                        ico_url = await download_and_upload_image(
                            ico_url, identifier="category"
                        )
            else:
                new_id = str(uuid.uuid4())
                code = None  # Ensure code is None for parent categories

            # Generate unique slug
            lvl_slug = await generate_unique_category_slug(db, level_name)

            category = Category(
                id=new_id,
                name=level_name,
                parent_id=parent_id,
                category_code=code,
                slug=lvl_slug,
                image_url=img_url,
                icon_url=ico_url,
            )
            db.add(category)
            await db.flush()

            existing[key] = category
            last_category_id = new_id
            parent_id = new_id
            created += 1

    await db.commit()
    msg = f"Import done. Created: {created}, updated: {updated} categories."
    if deleted > 0:
        msg += f" Deleted: {deleted} categories."
    return {"message": msg}


@router.get("/export-categories")
async def export_categories(
    db: Annotated[AsyncSession, Depends(get_db)],
    download_flag: Annotated[
        str,
        Query(description="File type to download: 'csv' or 'excel'. Default is 'excel'."),
    ] = "excel",
):
    # Fetch all categories
    result = await db.execute(select(Category))
    categories = result.scalars().all()

    # Build a parent map for path resolution
    category_map = {c.id: c for c in categories}

    def get_category_path(cat):
        path = []
        current = cat
        while current:
            path.append(current.name)
            if current.parent_id and current.parent_id in category_map:
                current = category_map[current.parent_id]
            else:
                current = None
        # Reverse to get Root > Child > Subchild
        return " > ".join(reversed(path))

    headers = ["Category Code", "Categories", "Image URL", "Icon URL"]
    rows = []

    for c in categories:
        # Ignore root/parent categories that just exist for grouping, usually we export all or specific
        code = c.category_code or ""
        img = c.image_url or ""
        icon = c.icon_url or ""
        path = get_category_path(c)
        rows.append([code, path, img, icon])

    import datetime

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    from service.product_import_export import write_file_for_response
    from fastapi.responses import StreamingResponse

    stream, filename, media = write_file_for_response(
        headers, rows, timestamp, download_flag
    )

    if download_flag == "excel":
        filename = f"categories_export_{timestamp}.xlsx"
    else:
        filename = f"categories_export_{timestamp}.csv"

    return StreamingResponse(
        stream,
        media_type=media,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# -----------------------------CATEGORY SEARCH HELPERS-----------------------------
def node_or_parent_matches(
    node_id: str,
    term: str,
    nodes: Dict[str, Dict[str, Any]],
) -> bool:
    cur = node_id
    seen = set()

    while cur and cur not in seen:
        seen.add(cur)
        n = nodes.get(cur)
        if not n:
            break

        name = (n["category_name"] or "").lower()
        if term in name:
            return True

        cur = n["parent_id"]

    return False


# -----------------------------------------CATEGORY SEARCH-----------------------------------------
@router.get("/categories/search")
async def search_categories(
    db: Annotated[AsyncSession, Depends(get_db)],
    q: Annotated[Optional[str], Query()] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
):
    offset = (page - 1) * limit
    search_term = q.strip().lower() if q else None

    result = await db.execute(select(Category))
    all_categories = result.scalars().all()  # list of ORM Category instances

    nodes: Dict[str, Dict[str, Any]] = {}
    for c in all_categories:
        cid = str(c.id)
        nodes[cid] = {
            "category_id": cid,
            "category_name": c.name,
            "category_is_active": c.is_active,
            "parent_id": str(c.parent_id) if c.parent_id is not None else None,
            "subcategories": [],
        }

    roots: List[Dict[str, Any]] = []
    for node in nodes.values():
        pid = node["parent_id"]
        if pid and pid in nodes:
            nodes[pid]["subcategories"].append(node)
        else:
            roots.append(node)

    def sort_tree(node: Dict[str, Any]):
        node["subcategories"].sort(key=lambda x: (x["category_name"] or "").lower())
        for child in node["subcategories"]:
            sort_tree(child)

    for r in roots:
        sort_tree(r)

    # 4) If search provided, keep only roots where node or any descendant matches
    def subtree_matches(node: Dict[str, Any], term: str) -> bool:
        if term in (node["category_name"] or "").lower():
            return True
        for ch in node["subcategories"]:
            if subtree_matches(ch, term):
                return True
        return False

    if search_term:
        filtered_roots = [r for r in roots if subtree_matches(r, search_term)]
    else:
        filtered_roots = roots

    total_roots = len(filtered_roots)
    pages = (total_roots + limit - 1) // limit

    # 5) Apply pagination to top-level roots (same as original)
    page_roots = filtered_roots[offset : offset + limit]

    # 6) Helper to strip parent_id when returning nested tree (keeps same output shape as original)
    def strip_parent_id(node: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "category_id": node["category_id"],
            "category_name": node["category_name"],
            "category_is_active": node["category_is_active"],
            "subcategories": [strip_parent_id(ch) for ch in node["subcategories"]],
        }

    nested_results = [strip_parent_id(r) for r in page_roots]

    def build_breadcrumb(node_id: str) -> str:
        parts: List[str] = []
        cur = node_id
        seen = set()
        while cur and cur not in seen:
            seen.add(cur)
            n = nodes.get(cur)
            if not n:
                break
            parts.append(n["category_name"] or "")
            cur = n["parent_id"]
        parts.reverse()
        return " → ".join(parts)

    flat_nodes = list(nodes.values())

    if search_term:
        flat_matches = [
            n
            for n in flat_nodes
            if node_or_parent_matches(n["category_id"], search_term, nodes)
        ]
    else:
        flat_matches = flat_nodes

    # Sort flat matches by name for stable ordering
    flat_matches.sort(key=lambda x: (x["category_name"] or "").lower())

    # Apply pagination to flat matches for Select2 (same page/limit)
    flat_offset = offset
    flat_page_slice = flat_matches[flat_offset : flat_offset + limit]

    select2_results = []
    # Pre-compute a quick set of parent_ids for has_children check
    parent_id_set = {n["parent_id"] for n in flat_nodes if n["parent_id"]}
    for n in flat_page_slice:
        cid = n["category_id"]
        text = build_breadcrumb(cid)
        has_children = cid in parent_id_set
        select2_results.append({"id": cid, "text": text, "has_children": has_children})

    select2_more = (flat_offset + len(flat_page_slice)) < len(flat_matches)

    return {
        "query": q,
        "page": page,
        "limit": limit,
        "total": total_roots,
        "pages": pages,
        "count": len(nested_results),
        "results": nested_results,
        "select2": {
            "results": select2_results,
            "pagination": {"more": select2_more},
            "total_matches": len(flat_matches),
        },
    }


# -----------------------------------------UPDATE CATEGORY STATUS-----------------------------------------
@router.patch(
    "/status/{category_id}",
    response_model=CategoryOutStatus,
    responses={
        404: {"description": "Category not found."},
    },
)
async def update_category_status(
    category_id: str,
    data: CategoryStatusUpdate,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    # Fetch category
    result = await db.execute(select(Category).where(Category.id == category_id))
    category = result.scalar_one_or_none()

    if not category:
        raise HTTPException(status_code=404, detail="Category not found")

    # Update status
    category.is_active = data.is_active
    category.updated_at = func.now()

    await db.commit()
    await db.refresh(category)

    return category


@router.get("/download-template")
async def download_category_template(
    file_type: Annotated[Literal["csv", "excel"], Query()] = "excel",
):
    headers = ["Category Code", "Categories", "Image URL", "Icon URL"]
    filename = "category_upload_template"

    if file_type == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}.csv"'},
        )

    # Excel
    output = io.BytesIO()
    wb = Workbook()
    ws = wb.active
    ws.title = "Categories"
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)
    wb.save(output)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}.xlsx"'},
    )
