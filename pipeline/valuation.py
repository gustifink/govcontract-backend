"""
Valuation & Scoring - Calculate impact ratios and filter signals

Fetches market data via yfinance and applies the "kill switch" rules.
"""
from decimal import Decimal
import yfinance as yf
from typing import Any

from config import get_settings

settings = get_settings()


def get_market_data(ticker: str) -> dict[str, Any] | None:
    """
    Fetch current market data for a ticker using yfinance.
    
    Returns:
        Dict with market_cap, avg_volume, or None if lookup fails
    """
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        market_cap = info.get("marketCap")
        
        # Kill switch: No market cap = probably private
        if not market_cap:
            return None
        
        return {
            "market_cap": market_cap,
            "avg_volume": info.get("averageVolume", 0),
            "sector": info.get("sector"),
            "current_price": info.get("currentPrice") or info.get("regularMarketPrice")
        }
    except Exception as e:
        print(f"⚠️  Failed to fetch data for {ticker}: {e}")
        return None


def calculate_impact_ratio(award_amount: float, market_cap: int) -> Decimal:
    """
    Calculate the impact ratio as a percentage.
    
    Impact Ratio = (Contract Value / Market Cap) × 100
    """
    if market_cap <= 0:
        return Decimal(0)
    
    ratio = (award_amount / market_cap) * 100
    return Decimal(str(round(ratio, 2)))


def apply_kill_switch(
    award_amount: float,
    market_cap: int | None,
    impact_ratio: Decimal
) -> tuple[bool, str]:
    """
    Apply filtering rules to determine if a signal should be discarded.
    
    Returns:
        Tuple of (should_keep, rejection_reason)
    """
    # No market cap = private company
    if market_cap is None:
        return False, "Private company (no market cap)"
    
    # Too big to move
    if market_cap > settings.max_market_cap:
        return False, f"Market cap ${market_cap/1e9:.1f}B exceeds ${settings.max_market_cap/1e9:.0f}B limit"
    
    # Impact too small
    if impact_ratio < settings.min_impact_ratio:
        return False, f"Impact ratio {impact_ratio}% below {settings.min_impact_ratio}% threshold"
    
    # Award too small (already filtered in ingestion, but double-check)
    if award_amount < settings.min_award_amount:
        return False, f"Award ${award_amount/1e6:.1f}M below ${settings.min_award_amount/1e6:.0f}M minimum"
    
    return True, ""


def score_signal(
    ticker: str,
    award_amount: float,
    potential_ceiling: float | None = None
) -> dict[str, Any] | None:
    """
    Full scoring pipeline for a matched contract.
    
    1. Fetch market data
    2. Calculate impact ratio
    3. Apply kill switch
    4. Return scored signal or None if filtered
    """
    # Get market data
    market_data = get_market_data(ticker)
    if not market_data:
        return None
    
    market_cap = market_data["market_cap"]
    
    # Calculate impact ratio
    impact_ratio = calculate_impact_ratio(award_amount, market_cap)
    
    # Apply kill switch
    keep, reason = apply_kill_switch(award_amount, market_cap, impact_ratio)
    
    if not keep:
        print(f"  ❌ {ticker}: {reason}")
        return None
    
    # Calculate ceiling impact (for IDV contracts)
    ceiling_impact = None
    if potential_ceiling and potential_ceiling > award_amount:
        ceiling_impact = calculate_impact_ratio(potential_ceiling, market_cap)
    
    return {
        "market_cap": market_cap,
        "avg_volume": market_data["avg_volume"],
        "sector": market_data["sector"],
        "impact_ratio": impact_ratio,
        "ceiling_impact": ceiling_impact
    }
