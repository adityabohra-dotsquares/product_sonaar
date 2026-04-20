from datetime import datetime, timedelta
from sqlalchemy import select, and_, or_, func, cast, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from typing import List, Optional, Any, Dict
from models.product import Product, ProductVariant, ProductStats
from models.brand import Brand
from models.category import Category
from models.return_policy import ReturnPolicy
from models.promotions import Promotion
from models.product_highlights import ProductHighlight, ProductHighlightItem
from models.stock_reservation import StockReservation
from schemas.product import ProductFilter
from repositories.base import BaseRepository

class ProductRepository(BaseRepository[Product]):
    def __init__(self, db: AsyncSession):
        super().__init__(db, Product)

    async def get_fuzzy_matched_ids(self, model: Any, field: Any, search_value: str) -> List[str]:
        normalized_search = search_value.strip().lower()
        result = await self.db.execute(select(model.id).where(func.lower(field).ilike(f"%{normalized_search}%")))
        return result.scalars().all()

    async def build_filters(self, filters: ProductFilter, is_searching: bool) -> List[Any]:
        conditions = []

        def field_filter(product_col, variant_col, value):
            if is_searching:
                conditions.append(or_(product_col == value, variant_col == value))
            else:
                conditions.append(product_col == value)

        if filters.status:
            field_filter(Product.status, ProductVariant.status, filters.status)

        if filters.fast_dispatch is not None:
            field_filter(Product.fast_dispatch, ProductVariant.fast_dispatch, filters.fast_dispatch)

        if filters.free_shipping is not None:
            field_filter(Product.free_shipping, ProductVariant.free_shipping, filters.free_shipping)

        if filters.vendor:
            conditions.append(Product.vendor_id.ilike(f"%{filters.vendor}%"))

        if filters.brand_id:
            brand_search = filters.brand_id.strip()
            conditions.append(Product.brand.has(Brand.name.ilike(f"%{brand_search}%")))

        if filters.ships_from:
            ships_search = filters.ships_from.strip()
            if is_searching:
                conditions.append(or_(Product.ships_from_location.ilike(f"%{ships_search}%"), ProductVariant.ships_from_location.ilike(f"%{ships_search}%")))
            else:
                conditions.append(Product.ships_from_location.ilike(f"%{ships_search}%"))

        if filters.tag:
            tag_search = filters.tag.strip()
            if is_searching:
                conditions.append(or_(
                    cast(Product.tags, String).ilike(f"%{tag_search}%"), 
                    cast(ProductVariant.tags, String).ilike(f"%{tag_search}%")
                ))
            else:
                conditions.append(cast(Product.tags, String).ilike(f"%{tag_search}%"))

        if filters.title:
            title_search = filters.title.strip()
            if is_searching:
                p_ids = await self.get_fuzzy_matched_ids(Product, Product.title, title_search)
                v_ids = await self.get_fuzzy_matched_ids(ProductVariant, ProductVariant.title, title_search)
                conditions.append(
                    or_(
                        Product.title.ilike(f"%{title_search}%"),
                        ProductVariant.title.ilike(f"%{title_search}%"),
                        Product.id.in_(p_ids),
                        ProductVariant.id.in_(v_ids)
                    )
                )
            else:
                p_ids = await self.get_fuzzy_matched_ids(Product, Product.title, title_search)
                conditions.append(or_(Product.title.ilike(f"%{title_search}%"), Product.id.in_(p_ids)))

        if filters.sku:
            sku_search = filters.sku.strip()
            if is_searching:
                conditions.append(or_(Product.sku.ilike(f"%{sku_search}%"), ProductVariant.sku.ilike(f"%{sku_search}%")))
            else:
                conditions.append(Product.sku.ilike(f"%{sku_search}%"))

        if filters.product_type:
            pt_search = filters.product_type.strip()
            if is_searching:
                conditions.append(or_(Product.product_type.ilike(f"%{pt_search}%"), ProductVariant.product_id_type.ilike(f"%{pt_search}%")))
            else:
                conditions.append(Product.product_type.ilike(f"%{pt_search}%"))

        if filters.q:
            q_lower = filters.q.lower().strip()
            if is_searching:
                p_ids = await self.get_fuzzy_matched_ids(Product, Product.title, q_lower)
                v_ids = await self.get_fuzzy_matched_ids(ProductVariant, ProductVariant.title, q_lower)
                conditions.append(
                    or_(
                        func.lower(Product.title).like(f"%{q_lower}%"),
                        func.lower(Product.sku).like(f"%{q_lower}%"),
                        func.lower(Product.unique_code).like(f"%{q_lower}%"),
                        func.lower(ProductVariant.title).like(f"%{q_lower}%"),
                        func.lower(ProductVariant.sku).like(f"%{q_lower}%"),
                        Product.id.in_(p_ids),
                        ProductVariant.id.in_(v_ids)
                    )
                )
            else:
                p_ids = await self.get_fuzzy_matched_ids(Product, Product.title, q_lower)
                conditions.append(
                    or_(
                        func.lower(Product.title).like(f"%{q_lower}%"),
                        func.lower(Product.sku).like(f"%{q_lower}%"),
                        func.lower(Product.unique_code).like(f"%{q_lower}%"),
                        Product.id.in_(p_ids)
                    )
                )

        if filters.ean:
            ean_search = filters.ean.strip()
            if is_searching:
                conditions.append(or_(Product.ean == ean_search, ProductVariant.ean == ean_search))
            else:
                conditions.append(Product.ean == ean_search)

        if filters.asin:
            asin_search = filters.asin.strip()
            if is_searching:
                conditions.append(or_(Product.asin == asin_search, ProductVariant.asin == asin_search))
            else:
                conditions.append(Product.asin == asin_search)

        if filters.mpn:
            mpn_search = filters.mpn.strip()
            if is_searching:
                conditions.append(or_(Product.mpn == mpn_search, ProductVariant.mpn == mpn_search))
            else:
                conditions.append(Product.mpn == mpn_search)

        if filters.unique_code:
            uc_search = filters.unique_code.strip()
            conditions.append(Product.unique_code.ilike(f"%{uc_search}%"))

        if filters.bundle_group_code:
            bgc_search = filters.bundle_group_code.strip()
            if is_searching:
                conditions.append(or_(Product.bundle_group_code.ilike(f"%{bgc_search}%"), ProductVariant.bundle_group_code.ilike(f"%{bgc_search}%")))
            else:
                conditions.append(Product.bundle_group_code.ilike(f"%{bgc_search}%"))

        if filters.condition:
            cond_search = filters.condition.strip()
            if len(cond_search) > 3 and "new" in cond_search.lower():
                cond_search = "New(open box)"
            if is_searching:
                conditions.append(or_(Product.product_condition == cond_search, ProductVariant.product_condition == cond_search))
            else:
                conditions.append(Product.product_condition == cond_search)

        if filters.todays_special:
            conditions.append(Product.highlight_items.any(
                ProductHighlightItem.highlight.has(ProductHighlight.type.in_(["Today's Deal", "Todays Deals"]))
            ))

        if filters.sales:
            now = datetime.now()
            conditions.append(Product.id.in_(
                select(Promotion.reference_id).where(
                    Promotion.offer_type == 'product',
                    Promotion.status == 'active',
                    Promotion.start_date <= now,
                    Promotion.end_date >= now
                )
            ))

        if filters.clearance:
            conditions.append(Product.highlight_items.any(
                ProductHighlightItem.highlight.has(ProductHighlight.type == "Clearance")
            ))

        if filters.limited_time_deal:
            conditions.append(Product.highlight_items.any(
                ProductHighlightItem.highlight.has(ProductHighlight.type.in_(["Hot Deals", "Trending Deals"]))
            ))

        if filters.new_arrivals:
            fifteen_days_ago = datetime.now() - timedelta(days=15)
            conditions.append(or_(
                Product.created_at >= fifteen_days_ago,
                Product.highlight_items.any(
                    ProductHighlightItem.highlight.has(ProductHighlight.type == "New Releases")
                )
            ))

        return conditions

    async def build_product_query(self, filters: ProductFilter):
        is_searching = any([
            filters.title, filters.sku, filters.q, filters.ean,
            filters.asin, filters.mpn, filters.unique_code
        ])
        conditions = await self.build_filters(filters, is_searching)

        query = select(Product)
        if is_searching:
            query = query.outerjoin(ProductVariant)

        query = query.options(
            selectinload(Product.images),
            selectinload(Product.brand),
            selectinload(Product.category),
            selectinload(Product.variants).selectinload(ProductVariant.images),
            selectinload(Product.variants).selectinload(ProductVariant.attributes),
        )

        query = query.where(*conditions)

        SORT_MAP = {
            "price_asc": Product.price.asc(),
            "price_desc": Product.price.desc(),
            "newly_added": Product.created_at.desc(),
            "oldest": Product.created_at.asc(),
            "stock_asc": Product.stock.asc(),
            "stock_desc": Product.stock.desc(),
        }

        order_by_clause = SORT_MAP.get(filters.sort_by, Product.created_at.desc())
        query = query.order_by(order_by_clause)

        return query

    async def get_reserved_stock_map(self, ids: List[str]) -> Dict[str, int]:
        if not ids:
            return {}
        stmt = (
            select(StockReservation.product_id, func.sum(StockReservation.quantity))
            .where(StockReservation.product_id.in_(ids))
            .where(StockReservation.status == "active")
            .group_by(StockReservation.product_id)
        )
        res = await self.db.execute(stmt)
        return {row[0]: (row[1] or 0) for row in res.all()}

    async def resolve_return_policy(self, product_id: str, country: Optional[str]) -> Optional[Dict]:
        now = datetime.now()
        conds = [
            ReturnPolicy.scope_type == "product",
            ReturnPolicy.scope_id == product_id,
            ReturnPolicy.status == "active",
            or_(ReturnPolicy.starts_at.is_(None), ReturnPolicy.starts_at <= now),
            or_(ReturnPolicy.ends_at.is_(None), ReturnPolicy.ends_at >= now),
        ]
        if country:
            conds.append(or_(ReturnPolicy.country_code == country, ReturnPolicy.country_code.is_(None)))

        stmt = (
            select(ReturnPolicy)
            .where(and_(*conds))
            .order_by(
                ((ReturnPolicy.country_code == country) if country else ReturnPolicy.country_code.is_(None)).desc(),
                ReturnPolicy.priority.desc(),
                func.coalesce(ReturnPolicy.starts_at, func.now()).desc(),
            )
            .limit(1)
        )
        row = (await self.db.execute(stmt)).scalars().first()
        if not row:
            return None
        return {
            "id": row.id,
            "country": row.country_code,
            "days": row.days,
            "restocking_fee_pct": row.restocking_fee_pct,
            "text": row.text,
            "priority": row.priority,
            "starts_at": row.starts_at.isoformat() if row.starts_at else None,
            "ends_at": row.ends_at.isoformat() if row.ends_at else None,
        }

    async def record_activity(self, product_id: str, view=False, order=False, cart=False):
        now = datetime.now()
        week_start = now - timedelta(days=now.weekday())
        month_start = datetime(now.year, now.month, 1)

        stmt = select(ProductStats).where(
            ProductStats.product_id == product_id,
            ProductStats.week_start == week_start,
            ProductStats.month_start == month_start
        )
        record = (await self.db.execute(stmt)).scalar_one_or_none()

        if not record:
            record = ProductStats(
                product_id=product_id,
                week_start=week_start,
                month_start=month_start,
                views=0, orders=0, added_to_cart=0
            )
            self.db.add(record)

        record.views = (record.views or 0) + (1 if view else 0)
        record.orders = (record.orders or 0) + (1 if order else 0)
        record.added_to_cart = (record.added_to_cart or 0) + (1 if cart else 0)
        await self.db.flush()
