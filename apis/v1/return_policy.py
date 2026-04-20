from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from uuid import uuid4
from typing import Annotated

from models.return_policy import ReturnPolicy
from schemas.return_policy import (
    ReturnPolicyCreate,
    ReturnPolicyUpdate,
    ReturnPolicyOut,
)
from deps import get_db

router = APIRouter()


@router.post(
    "/create-policy",
    response_model=ReturnPolicyOut,
    responses={
        400: {"description": "A policy already exists for this scope."},
    },
)
async def create_policy(
    payload: ReturnPolicyCreate, db: Annotated[AsyncSession, Depends(get_db)]
):
    # 1️⃣ Check if a policy with same scope_type, scope_id, country already exists
    stmt = select(ReturnPolicy).where(
        and_(
            ReturnPolicy.scope_type == payload.scope_type,
            ReturnPolicy.scope_id == payload.scope_id,
            ReturnPolicy.country_code == payload.country_code,
        )
    )
    existing = (await db.execute(stmt)).scalars().first()

    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"A policy already exists for this {payload.scope_type}"
            f"{' (' + payload.country_code + ')' if payload.country_code else ''}.",
        )

    # 2️⃣ Create new policy
    policy = ReturnPolicy(**payload.dict())
    db.add(policy)
    await db.commit()
    await db.refresh(policy)
    return policy


@router.get("/list-policies", response_model=list[ReturnPolicyOut])
async def list_policies(db: Annotated[AsyncSession, Depends(get_db)]):
    result = await db.execute(select(ReturnPolicy))
    return result.scalars().all()


@router.get(
    "/get-policy/{policy_id}",
    response_model=ReturnPolicyOut,
    responses={
        404: {"description": "Return policy not found"},
    },
)
async def get_policy(policy_id: str, db: Annotated[AsyncSession, Depends(get_db)]):
    policy = await db.get(ReturnPolicy, policy_id)
    if not policy:
        raise HTTPException(404, "Return policy not found")
    return policy


@router.put(
    "/update-policy/{policy_id}",
    response_model=ReturnPolicyOut,
    responses={
        404: {"description": "Return policy not found"},
    },
)
async def update_policy(
    policy_id: str,
    payload: ReturnPolicyUpdate,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    policy = await db.get(ReturnPolicy, policy_id)
    if not policy:
        raise HTTPException(404, "Return policy not found")

    for key, value in payload.dict(exclude_unset=True).items():
        setattr(policy, key, value)

    await db.commit()
    await db.refresh(policy)
    return policy


@router.delete(
    "/delete-policy/{policy_id}",
    responses={
        404: {"description": "Return policy not found"},
    },
)
async def delete_policy(policy_id: str, db: Annotated[AsyncSession, Depends(get_db)]):
    policy = await db.get(ReturnPolicy, policy_id)
    if not policy:
        raise HTTPException(404, "Return policy not found")
    await db.delete(policy)
    await db.commit()
    return {"message": "Policy deleted successfully"}
