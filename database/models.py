"""
SQLAlchemy models for GovContract-Alpha
"""
from datetime import datetime
from decimal import Decimal
from sqlalchemy import String, BigInteger, Text, DECIMAL, TIMESTAMP, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from database.connection import Base


class Company(Base):
    """
    Companies table - Static reference of publicly traded companies.
    Updated weekly via yfinance bulk fetch.
    """
    __tablename__ = "companies"

    ticker: Mapped[str] = mapped_column(String(10), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    name_normalized: Mapped[str | None] = mapped_column(String(255), index=True)
    market_cap: Mapped[int | None] = mapped_column(BigInteger)
    avg_volume: Mapped[int | None] = mapped_column(BigInteger)
    sector: Mapped[str | None] = mapped_column(String(50))
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationship
    signals: Mapped[list["Signal"]] = relationship(back_populates="company")

    def __repr__(self):
        return f"<Company {self.ticker}: {self.name}>"


class Signal(Base):
    """
    Signals table - The product. High-impact government contract wins.
    """
    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    contract_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    ticker: Mapped[str] = mapped_column(
        String(10), ForeignKey("companies.ticker"), nullable=False
    )
    agency_name: Mapped[str | None] = mapped_column(String(255))
    contract_description: Mapped[str | None] = mapped_column(Text)
    award_amount: Mapped[Decimal] = mapped_column(DECIMAL(20, 2), nullable=False)
    potential_ceiling: Mapped[Decimal | None] = mapped_column(DECIMAL(20, 2))
    market_cap_at_time: Mapped[Decimal | None] = mapped_column(DECIMAL(20, 2))
    impact_ratio: Mapped[Decimal] = mapped_column(DECIMAL(5, 2), nullable=False)
    contract_date: Mapped[datetime | None] = mapped_column(TIMESTAMP)
    sam_gov_url: Mapped[str | None] = mapped_column(Text)
    detected_at: Mapped[datetime] = mapped_column(TIMESTAMP, default=datetime.utcnow)
    
    # Stock price at contract announcement time
    price_at_contract: Mapped[Decimal | None] = mapped_column(DECIMAL(12, 4))
    
    # Price BEFORE announcement (% change from that time to announcement)
    price_before_1h: Mapped[Decimal | None] = mapped_column(DECIMAL(8, 2))   # % change 1h before
    price_before_6h: Mapped[Decimal | None] = mapped_column(DECIMAL(8, 2))   # % change 6h before
    price_before_24h: Mapped[Decimal | None] = mapped_column(DECIMAL(8, 2))  # % change 24h before
    
    # Price AFTER announcement (% change from announcement to that time)
    price_after_1m: Mapped[Decimal | None] = mapped_column(DECIMAL(8, 2))    # % change 1min after
    price_after_1h: Mapped[Decimal | None] = mapped_column(DECIMAL(8, 2))    # % change 1h after
    price_after_6h: Mapped[Decimal | None] = mapped_column(DECIMAL(8, 2))    # % change 6h after
    price_after_24h: Mapped[Decimal | None] = mapped_column(DECIMAL(8, 2))   # % change 24h after

    # Relationship
    company: Mapped["Company"] = relationship(back_populates="signals")

    # Indexes for performance
    __table_args__ = (
        Index("idx_signals_detected_at", detected_at.desc()),
        Index("idx_signals_contract_date", contract_date.desc()),
        Index("idx_signals_impact_ratio", impact_ratio.desc()),
    )

    def __repr__(self):
        return f"<Signal {self.ticker}: ${self.award_amount:,.0f} ({self.impact_ratio}%)>"


