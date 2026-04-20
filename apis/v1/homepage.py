from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from typing import List, Optional, Annotated

from deps import get_db
from models.homepage import HomepageSection
from schemas.homepage import HomepageSectionCreate, HomepageSectionUpdate, HomepageSectionRead

router = APIRouter()


HOMEPAGE_SECTION_TYPES = {
    "top-categories": "Top Categories",
    "hero_banner": "Hero Banner",
    "product_carousel": "Product Carousel",
    "category_grid": "Category Grid",
    "featured_products": "Featured Products",
    "new_arrivals": "New Arrivals",
    "bestsellers": "Best Sellers",
    "testimonials": "Testimonials",
    "brands": "Brands Carousel",
    "newsletter": "Newsletter Signup"
}

# --- Create ---
@router.post(
    "/",
    response_model=HomepageSectionRead,
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {"description": "Section with this type already exists"},
    },
)
async def create_homepage_section(
    section: HomepageSectionCreate, db: Annotated[AsyncSession, Depends(get_db)]
):
    # check if type exists:
    stmt = select(HomepageSection).where(HomepageSection.type == section.type)
    result = await db.execute(stmt)
    if result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Section with this type already exists"
        )

    new_section = HomepageSection(
        type=section.type,
        title=section.title,
        position=section.position,
        is_active=section.is_active,
        config=section.config,
        start_at=section.start_at,
        end_at=section.end_at
    )
    db.add(new_section)
    await db.commit()
    await db.refresh(new_section)
    return new_section

# --- Read All ---
@router.get("/", response_model=List[HomepageSectionRead])
async def list_homepage_sections(
    db: Annotated[AsyncSession, Depends(get_db)],
    is_active: Annotated[
        Optional[bool], Query(description="Filter by active status")
    ] = None,
):
    query = select(HomepageSection).order_by(HomepageSection.position.asc())
    
    if is_active is not None:
        query = query.where(HomepageSection.is_active == is_active)
        
    result = await db.execute(query)
    return result.scalars().all()

# --- Read One ---
@router.get(
    "/{type}",
    response_model=HomepageSectionRead,
    responses={
        404: {"description": "Section not found"},
    },
)
async def get_homepage_section(type: str, db: Annotated[AsyncSession, Depends(get_db)]):
    result = await db.execute(select(HomepageSection).where(HomepageSection.type == type))
    section = result.scalar_one_or_none()
    
    if not section:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Section not found")
        
    return section

# --- Update ---
@router.patch(
    "/{type}",
    response_model=HomepageSectionRead,
    responses={
        400: {"description": "Section with this type already exists"},
        404: {"description": "Section not found"},
    },
)
async def update_homepage_section(
    type: str,
    updates: HomepageSectionUpdate,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(select(HomepageSection).where(HomepageSection.type == type))
    section = result.scalar_one_or_none()
    
    if not section:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Section not found")
    
    update_data = updates.dict(exclude_unset=True)
    
    if "type" in update_data:
        # check if other section has same type
        stmt = select(HomepageSection).where(
            HomepageSection.type == update_data["type"],
            HomepageSection.id != section.id
        )
        result = await db.execute(stmt)
        if result.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Section with this type already exists"
            )

    for key, value in update_data.items():
        setattr(section, key, value)
        
    await db.commit()
    await db.refresh(section)
    return section

# --- Delete ---
@router.delete(
    "/{type}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        404: {"description": "Section not found"},
    },
)
async def delete_homepage_section(
    type: str,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(select(HomepageSection).where(HomepageSection.type == type))
    section = result.scalar_one_or_none()
    
    if not section:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Section not found")
        
    await db.delete(section)
    await db.commit()
    return None




# // {
# //     // "title": "",
# //     "type": "hero_banner",
# //     "position": "1",
# //     "is_active": true,
# //     "config": {
# //         "items": [
# //             {
# //                 "content": [
# //                     {
# //                         "title": "Discover Superior Sound",
# //                         "subtitle": "HEADPHONES",
# //                         "image": "https://canyon.eu/blog/wp-content/uploads/2023/10/19572scr_7bc84362e44362e.jpg",
# //                         // "mobile_image": "hero_1_mobile.jpg",
# //                         "cta_text": "Shop Now",
# //                         "cta_link": "/category/headphones"
# //                     },
# //                     {
# //                         "title": "Discover Superior Sound",
# //                         "subtitle": "HEADPHONES",
# //                         "image": "https://images.pexels.com/photos/610945/pexels-photo-610945.jpeg",
# //                         // "mobile_image": "hero_1_mobile.jpg",
# //                         "cta_text": "Shop Now",
# //                         "cta_link": "/category/headphones"
# //                     },
# //                     {
# //                         "title": "Discover Superior Sound",
# //                         "subtitle": "HEADPHONES",
# //                         "image": "https://images.pexels.com/photos/32200886/pexels-photo-32200886.jpeg?cs=srgb&dl=pexels-armorshop-32200886.jpg&fm=jpg",
# //                         // "mobile_image": "hero_1_mobile.jpg",
# //                         "cta_text": "Shop Now",
# //                         "cta_link": "/category/headphones"
# //                     }
# //                 ]
# //             },
# //             {
# //                 "content": [
# //                     {
# //                         "title": "Discover Superior Sound",
# //                         "subtitle": "HEADPHONES",
# //                         "image": "https://images.pexels.com/photos/89955/pexels-photo-89955.jpeg?cs=srgb&dl=pexels-deyvi-romero-15310-89955.jpg&fm=jpg",
# //                         // "mobile_image": "hero_1_mobile.jpg",
# //                         "cta_text": "Shop Now",
# //                         "cta_link": "/category/headphones"
# //                     },
# //                     {
# //                         "title": "Discover Superior Sound",
# //                         "subtitle": "HEADPHONES",
# //                         "image": "https://images.pexels.com/photos/1542252/pexels-photo-1542252.jpeg?auto=compress&cs=tinysrgb&dpr=1&w=500",
# //                         // "mobile_image": "hero_1_mobile.jpg",
# //                         "cta_text": "Shop Now",
# //                         "cta_link": "/category/headphones"
# //                     },
# //                     {
# //                         "title": "Discover Superior Sound",
# //                         "subtitle": "HEADPHONES",
# //                         "image": "https://images.pexels.com/photos/404280/pexels-photo-404280.jpeg?cs=srgb&dl=pexels-noah-erickson-97554-404280.jpg&fm=jpg",
# //                         // "mobile_image": "hero_1_mobile.jpg",
# //                         "cta_text": "Shop Now",
# //                         "cta_link": "/category/headphones"
# //                     }
# //                 ]
# //             },
# //             {
# //                 "content": [
# //                     {
# //                         "title": "Discover Superior Sound",
# //                         "subtitle": "HEADPHONES",
# //                         "image": "https://images.pexels.com/photos/1055691/pexels-photo-1055691.jpeg?cs=srgb&dl=pexels-kowalievska-1055691.jpg&fm=jpg",
# //                         // "mobile_image": "hero_1_mobile.jpg",
# //                         "cta_text": "Shop Now",
# //                         "cta_link": "/category/headphones"
# //                     },
# //                     {
# //                         "title": "Discover Superior Sound",
# //                         "subtitle": "HEADPHONES",
# //                         "image": "https://media.istockphoto.com/id/1209951015/photo/young-woman-wearing-raincoat.jpg?s=612x612&w=0&k=20&c=wu5wvtTg3zXhYNFpYKo3QI_biQbO3iO8bJJbXvv0Auw=",
# //                         // "mobile_image": "hero_1_mobile.jpg",
# //                         "cta_text": "Shop Now",
# //                         "cta_link": "/category/headphones"
# //                     }
# //                 ]
# //             },
# //             {
# //                 "content": [
# //                     {
# //                         "title": "Discover Superior Sound",
# //                         "subtitle": "HEADPHONES",
# //                         "image": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQOgNBumWTwNiU3sVUGjAsGP67UTR82wAg7Vw&s",
# //                         // "mobile_image": "hero_1_mobile.jpg",
# //                         "cta_text": "Shop Now",
# //                         "cta_link": "/category/headphones"
# //                     },
# //                     {
# //                         "title": "Discover Superior Sound",
# //                         "subtitle": "HEADPHONES",
# //                         "image": "https://images.pexels.com/photos/7432/pexels-photo.jpg?cs=srgb&dl=pexels-jeshoots-com-147458-7432.jpg&fm=jpg",
# //                         // "mobile_image": "hero_1_mobile.jpg",
# //                         "cta_text": "Shop Now",
# //                         "cta_link": "/category/headphones"
# //                     },
# //                     {
# //                         "title": "Discover Superior Sound",
# //                         "subtitle": "HEADPHONES",
# //                         "image": "https://images.pexels.com/photos/34151788/pexels-photo-34151788.jpeg?cs=srgb&dl=pexels-idean-azad-2150674828-34151788.jpg&fm=jpg",
# //                         // "mobile_image": "hero_1_mobile.jpg",
# //                         "cta_text": "Shop Now",
# //                         "cta_link": "/category/headphones"
# //                     }
# //                 ]
# //             }
# //         ]
# //     }
# // }