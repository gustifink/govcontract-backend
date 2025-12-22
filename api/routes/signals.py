"""
Signal routes - The main product endpoints
"""
from datetime import datetime
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database import get_db, Signal, Company
from api.schemas import SignalListItem, SignalDetail, SignalListResponse

router = APIRouter(prefix="/signals", tags=["Signals"])


def get_impact_tier(ratio: Decimal) -> str:
    """Convert impact ratio to tier label"""
    if ratio >= 20:
        return "nuclear"
    elif ratio >= 10:
        return "high"
    else:
        return "moderate"


@router.get("", response_model=SignalListResponse)
async def list_signals(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    min_impact: float = Query(None, ge=0, description="Minimum impact ratio filter"),
    ticker: str = Query(None, description="Filter by specific ticker"),
    sort_by: str = Query("contract_date", description="Sort by: contract_date or detected_at"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get paginated list of signals for the live feed.
    Supports sorting by contract_date (when contract was signed) or detected_at (when we found it).
    """
    # Base query with company join
    query = select(Signal).options(selectinload(Signal.company))
    
    # Apply sorting
    if sort_by == "detected_at":
        query = query.order_by(desc(Signal.detected_at))
    else:  # Default to contract_date
        query = query.order_by(desc(Signal.contract_date))
    
    count_query = select(func.count(Signal.id))
    
    # Apply filters
    if min_impact is not None:
        query = query.where(Signal.impact_ratio >= min_impact)
        count_query = count_query.where(Signal.impact_ratio >= min_impact)
    
    if ticker:
        query = query.where(Signal.ticker == ticker.upper())
        count_query = count_query.where(Signal.ticker == ticker.upper())
    
    # Get total count
    total = await db.scalar(count_query)
    
    # Paginate
    offset = (page - 1) * page_size
    query = query.offset(offset).limit(page_size)
    
    result = await db.execute(query)
    signals = result.scalars().all()
    
    # Transform to response with company names, contract dates, and price evolution
    items = [
        SignalListItem(
            id=s.id,
            ticker=s.ticker,
            company_name=s.company.name if s.company else None,
            impact_ratio=s.impact_ratio,
            impact_tier=get_impact_tier(s.impact_ratio),
            award_amount=s.award_amount,
            market_cap_at_time=s.market_cap_at_time,
            agency_name=s.agency_name,
            contract_date=s.contract_date,
            detected_at=s.detected_at,
            price_at_contract=s.price_at_contract,
            # Price BEFORE announcement
            price_before_1h=s.price_before_1h,
            price_before_6h=s.price_before_6h,
            price_before_24h=s.price_before_24h,
            # Price AFTER announcement
            price_after_1m=s.price_after_1m,
            price_after_1h=s.price_after_1h,
            price_after_6h=s.price_after_6h,
            price_after_24h=s.price_after_24h,
        )
        for s in signals
    ]
    
    pages = (total + page_size - 1) // page_size if total else 0
    
    return SignalListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        pages=pages
    )


@router.get("/{signal_id}", response_model=SignalDetail)
async def get_signal(
    signal_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Get full signal details for the modal view.
    Includes company info and SAM.gov link.
    """
    query = (
        select(Signal)
        .options(selectinload(Signal.company))
        .where(Signal.id == signal_id)
    )
    
    result = await db.execute(query)
    signal = result.scalar_one_or_none()
    
    if not signal:
        raise HTTPException(status_code=404, detail="Signal not found")
    
    return signal
