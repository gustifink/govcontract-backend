"""
Company routes
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db, Company
from api.schemas import CompanyResponse

router = APIRouter(prefix="/companies", tags=["Companies"])


@router.get("/{ticker}", response_model=CompanyResponse)
async def get_company(
    ticker: str,
    db: AsyncSession = Depends(get_db)
):
    """Get company details by ticker"""
    query = select(Company).where(Company.ticker == ticker.upper())
    result = await db.execute(query)
    company = result.scalar_one_or_none()
    
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    
    return company


@router.get("", response_model=list[CompanyResponse])
async def search_companies(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_db)
):
    """Search companies by name or ticker"""
    search_term = f"%{q.upper()}%"
    query = (
        select(Company)
        .where(
            (Company.ticker.ilike(search_term)) | 
            (Company.name.ilike(search_term))
        )
        .limit(limit)
    )
    
    result = await db.execute(query)
    return result.scalars().all()
