from models.review import Review, ReviewStats
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select


async def update_review_stats(db: AsyncSession, product_id: str):
    # Get all reviews for this product
    result = await db.execute(
        select(Review.rating).where(Review.product_id == product_id)
    )
    ratings = [r[0] for r in result.fetchall()]

    if not ratings:
        avg_rating = 0.0
        total_reviews = 0
        counts = [0, 0, 0, 0, 0]
    else:
        avg_rating = sum(ratings) / len(ratings)
        total_reviews = len(ratings)
        counts = [
            len([r for r in ratings if r == 5]),
            len([r for r in ratings if r == 4]),
            len([r for r in ratings if r == 3]),
            len([r for r in ratings if r == 2]),
            len([r for r in ratings if r == 1]),
        ]

    # Check if a ReviewStats row already exists
    stats_result = await db.execute(
        select(ReviewStats).where(ReviewStats.product_id == product_id)
    )
    stats = stats_result.scalar_one_or_none()

    if stats:
        stats.average_rating = avg_rating
        stats.total_reviews = total_reviews
        (
            stats.five_star_count,
            stats.four_star_count,
            stats.three_star_count,
            stats.two_star_count,
            stats.one_star_count,
        ) = counts
    else:
        stats = ReviewStats(
            product_id=product_id,
            average_rating=avg_rating,
            total_reviews=total_reviews,
            five_star_count=counts[0],
            four_star_count=counts[1],
            three_star_count=counts[2],
            two_star_count=counts[3],
            one_star_count=counts[4],
        )
        db.add(stats)

    await db.commit()
