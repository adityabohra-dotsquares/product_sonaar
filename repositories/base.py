from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import TypeVar, Generic, Type, List, Optional
from database import Base

T = TypeVar("T", bound=Base)

class BaseRepository(Generic[T]):
    def __init__(self, db: AsyncSession, model: Type[T]):
        self.db = db
        self.model = model

    async def get_by_id(self, id: str, includes: List = None) -> Optional[T]:
        stmt = select(self.model).where(self.model.id == id)
        if includes:
            stmt = stmt.options(*includes)
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def get_all(self, includes: List = None) -> List[T]:
        stmt = select(self.model)
        if includes:
            stmt = stmt.options(*includes)
        result = await self.db.execute(stmt)
        return result.scalars().unique().all()

    def add(self, entity: T):
        self.db.add(entity)

    async def delete(self, entity: T):
        await self.db.delete(entity)

    async def flush(self):
        await self.db.flush()

    async def commit(self):
        await self.db.commit()

    async def refresh(self, entity: T, attribute_names: List[str] = None):
        await self.db.refresh(entity, attribute_names=attribute_names)
