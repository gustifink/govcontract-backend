"""
Pydantic schemas for API request/response models
"""
from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, Field
from typing import Literal


# ============== Company Schemas ==============

class CompanyBase(BaseModel):
    ticker: str
    name: str
    market_cap: int | None = None
    sector: str | None = None


class CompanyResponse(CompanyBase):
    avg_volume: int | None = None
    updated_at: datetime

    class Config:
        from_attributes = True


# ============== Signal Schemas ==============

class SignalBase(BaseModel):
    contract_id: str
    ticker: str
    agency_name: str | None = None
    contract_description: str | None = None
    award_amount: Decimal
    potential_ceiling: Decimal | None = None
    impact_ratio: Decimal


class SignalListItem(BaseModel):
    """Compact signal for the live feed table"""
    id: int
    ticker: str
    company_name: str | None = None
    impact_ratio: Decimal = Field(description="Impact as percentage, e.g. 15.50")
    impact_tier: Literal["nuclear", "high", "moderate"] = Field(
        description="🟢 nuclear (>20%), 🟡 high (10-20%), ⚪ moderate (5-10%)"
    )
    award_amount: Decimal
    market_cap_at_time: Decimal | None
    agency_name: str | None
    contract_date: datetime | None = None
    detected_at: datetime
    price_at_contract: Decimal | None = None
    
    # Price BEFORE announcement (% changes)
    price_before_1h: Decimal | None = None
    price_before_6h: Decimal | None = None
    price_before_24h: Decimal | None = None
    
    # Price AFTER announcement (% changes)
    price_after_1m: Decimal | None = None
    price_after_1h: Decimal | None = None
    price_after_6h: Decimal | None = None
    price_after_24h: Decimal | None = None

    class Config:
        from_attributes = True


class SignalDetail(SignalBase):
    """Full signal details for the modal view"""
    id: int
    market_cap_at_time: Decimal | None
    potential_ceiling: Decimal | None
    contract_date: datetime | None
    sam_gov_url: str | None
    detected_at: datetime
    company: CompanyResponse | None = None

    class Config:
        from_attributes = True


# ============== Pagination ==============

class PaginatedResponse(BaseModel):
    items: list
    total: int
    page: int
    page_size: int
    pages: int


class SignalListResponse(PaginatedResponse):
    items: list[SignalListItem]


# ============== Pipeline ==============

class PipelineStatus(BaseModel):
    status: Literal["running", "idle", "error"]
    last_run: datetime | None
    contracts_processed: int = 0
    signals_created: int = 0
    next_run: datetime | None = None
