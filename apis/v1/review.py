from fastapi import APIRouter, Depends, HTTPException, status
from service.review_stats import update_review_stats
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from models.review import Review, ReviewStats
from models.product import Product, ProductVariant
from schemas.review import ReviewCreate, ReviewOut, ReviewUpdate
from schemas.product import ProductResponse, ReviewStatsResponse
from deps import get_db
from sqlalchemy import desc, func
from typing import List, Annotated
from models.brand import Brand
from models.category import Category

router = APIRouter()


# ---------------- CREATE REVIEW ----------------
@router.post(
    "/create-review/{product_id}",
    response_model=ReviewOut,
    responses={
        400: {"description": "Review already submitted for this order"},
        404: {"description": "Product not found"},
    },
)
async def create_review(
    product_id: str,
    review: ReviewCreate,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    # 1️⃣ Product exists
    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    # 2️⃣ Prevent duplicate review per order
    exists = await db.execute(
        select(Review).where(
            Review.product_id == product_id,
            Review.user_id == review.user_id,
            Review.order_id == review.order_id,
        )
    )
    if exists.scalar_one_or_none():
        raise HTTPException(
            status_code=400,
            detail="Review already submitted for this order",
        )

    # 3️⃣ Create review (trusted order service)
    new_review = Review(
        reviewer_name=review.reviewer_name,
        product_identifier=product_id,
        product_id=product_id,
        user_id=review.user_id,
        order_id=review.order_id,
        rating=review.rating,
        comment=review.comment,
        title=review.title,  
        images=review.images, 
        review_type="customer",
        is_verified_purchase=True,
    )

    db.add(new_review)
    await db.commit()
    await db.refresh(new_review)

    await update_review_stats(db, product_id)

    return new_review


# ---------------- GET REVIEWS FOR PRODUCT ----------------
@router.get("/reviews/{product_id}", response_model=list[ReviewOut])
async def get_reviews(product_id: str, db: Annotated[AsyncSession, Depends(get_db)]):
    result = await db.execute(
        select(Review)
        .where(Review.product_id == product_id)
        .options(selectinload(Review.variant), selectinload(Review.product))
    )
    reviews = result.scalars().all()
    return reviews


# ---------------- DELETE REVIEW ----------------
@router.delete(
    "/delete-review/{review_id}",
    responses={
        404: {"description": "Review not found"},
    },
)
async def delete_review(review_id: str, db: Annotated[AsyncSession, Depends(get_db)]):
    result = await db.execute(select(Review).where(Review.id == review_id))
    review = result.scalar_one_or_none()

    if not review:
        raise HTTPException(status_code=404, detail="Review not found")

    product_id = review.product_id

    await db.delete(review)
    await db.commit()

    await update_review_stats(db, product_id)

    return {"status": "success", "message": "Review deleted successfully"}


# ---------------- UPDATE REVIEW ----------------
@router.put(
    "/update-review/{review_id}",
    responses={
        404: {"description": "Review not found"},
    },
)
async def update_review(
    review_id: str, review_data: ReviewUpdate, db: Annotated[AsyncSession, Depends(get_db)]
):
    """
    Update an existing review by its ID
    """

    result = await db.execute(select(Review).where(Review.id == review_id))
    review = result.scalar_one_or_none()

    if not review:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Review not found"
        )

    for field, value in review_data.dict(exclude_unset=True).items():
        setattr(review, field, value)

    await db.commit()
    await db.refresh(review)

    return {
        "message": "Review updated successfully",
        "review": {
            "id": review.id,
            "product_id": review.product_id,
            "reviewer_name": review.reviewer_name,
            "rating": review.rating,
            "comment": review.comment,
            "updated_at": review.updated_at,
        },
    }


# ---------------- GET PRODUCTS WITH REVIEWS ----------------
@router.get("/products-with-reviews", response_model=List[ProductResponse])
async def get_products_with_reviews(
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: int = 10,
    sort_by: str = "most_reviews",
):
    """
    Get products that have at least one review.
    Populates full product details including variants.
    Aggregates review counts and ratings directly from the Reviews table.
    
    sort_by options: "most_reviews" (default), "latest"
    """
    
    # Determine sort order
    if sort_by == "latest":
        order_clause = Product.created_at.desc()
    else:
        # Default to most reviews
        order_clause = desc("total_reviews")

    # Query: Select Product, Count(Reviews), Avg(Rating)
    stmt = (
        select(
            Product,
            func.count(Review.id).label("total_reviews"),
            func.avg(Review.rating).label("average_rating"),
        )
        .join(Review, Review.product_id == Product.id)
        .group_by(Product.id)
        .having(func.count(Review.id) > 0)
        .order_by(order_clause)
        .limit(limit)
        .options(
            selectinload(Product.variants).selectinload(ProductVariant.attributes),
            selectinload(Product.variants).selectinload(ProductVariant.images),
            selectinload(Product.brand),
            selectinload(Product.category),
            selectinload(Product.images),
            # selectinload(Product.review_stats), # Not loading review_stats model, calculating it
            selectinload(Product.seo),
        )
    )

    result = await db.execute(stmt)
    rows = result.all()

    enriched_products = []
    for row in rows:
        product = row[0]
        total_reviews = row[1]
        average_rating = row[2]

        # Populate manual fields
        # Create a dict or a temporary object to hold the data
        # We can't modify the ORM object's relationship with a Pydantic model
        
        # Using a trick: The response model will pull data from attributes. 
        # We can set a temporary attribute ON THE INSTANCE that is NOT the relationship name if we change the schema alias,
        # OR we can just convert to dict. Converting to dict is safer.
        
        product_data = product.__dict__.copy() # shallow copy of columns
        
        # Add relationship data that was eagerly loaded
        if product.brand:
            product_data["brand_name"] = product.brand.name
        if product.category:
            product_data["category_name"] = product.category.name
        
        # Variants, Images, SEO are lists/objects, they should be in the dict if accessed or we need to ensure they are accessible.
        # SQLAlchemy's __dict__ might not have loaded relationships if they are proxies.
        # Safer approach: Let Pydantic validate the ORM object, then we update the result? No, that's double validation.
        # Better: Assign to a NON-ORM attribute if possible, but Pydantic config `from_attributes=True` looks for attributes.
        
        # Let's try constructing a wrapper or just simple attribute assignment to a NEW name if schema allows, 
        # BUT the schema expects `review_stats`.
        
        # The issue is `product.review_stats` is an InstrumentedAttribute (relationship).
        # We can't overwrite it with a Pydantic model.
        # BUT we can just construct the response model directly? NO, nested models are complex.
        
        # Alternative: We can attach it to the product object using a DIFFERENT name, 
        # but our schema expects `review_stats`.
        
        # WORKAROUND: Create a mock object or use `setattr` on the instance for a 'transient' property if `review_stats` wasn't a relationship? 
        # It IS a relationship.
        
        # Let's just create a dictionary representation for the `enriched_product`
        # We need to manually handle the nested relationships if we go the dict route.
        # OR...
        # We can instantiate the `ProductResponse` manually. 
        # This gives us full control.
        
        # Let's use `jsonable_encoder` or just basic dict comprehension to be safe and explicit.
        # Actually... `product` is a SQLAlchemy object.
        # Let's try:
        # product.brand_name = product.brand.name if product.brand else None
        # product.category_name = product.category.name if product.category else None
        
        # Create the stats object
        stats_data = ReviewStatsResponse(
            average_rating=average_rating or 0.0,
            total_reviews=total_reviews or 0,
            five_star_count=0,
            four_star_count=0,
            three_star_count=0,
            two_star_count=0,
            one_star_count=0,
        )
        
        # We simply return a structure that Pydantic can parse. 
        # A simple class that wraps the product and overrides review_stats?
        # Or just MonkeyPatching? 
        # MonkeyPatching the instance `product.review_stats` fails because of SQLAlchemy instrumentation.
        # But we can iterate and create a dict.
        
        # Let's go with the manually constructed response logic, but we need to preserve all other fields.
        # The `ProductResponse` is quite large.
        
        # Hacky but effective fix: 
        # Don't use the ORM object 'product' directly in the list.
        # Create a new object that looks like it.
        class ProductWrapper:
            def __init__(self, product_orm, stats):
                self._product = product_orm
                self.review_stats = stats
            
            def __getattr__(self, name):
                return getattr(self._product, name)

        enriched_products.append(ProductWrapper(product, stats_data))

    return enriched_products
