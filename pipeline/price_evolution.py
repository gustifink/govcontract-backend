"""
Stock Price Evolution Tracker

Fetches historical price data around contract dates to measure stock reaction.
Uses yfinance for price data.
"""
from datetime import datetime, timedelta
from decimal import Decimal
import yfinance as yf
from typing import Any


def get_price_evolution(ticker: str, contract_date: datetime) -> dict[str, Any]:
    """
    Get stock price evolution around a contract date.
    
    Returns:
        Dict with price_at_contract and % changes: before_7d, after_1d, after_7d
        Note: Intraday data (1h, 6h) only available for last 7 days
    """
    result = {
        "price_at_contract": None,
        "price_before_7d": None,
        "price_after_1h": None,
        "price_after_6h": None,
        "price_after_1d": None,
        "price_after_7d": None,
    }
    
    try:
        stock = yf.Ticker(ticker)
        
        # Get 30-day history around contract date
        start = contract_date - timedelta(days=14)
        end = min(contract_date + timedelta(days=14), datetime.now())
        
        hist = stock.history(start=start, end=end, interval="1d")
        
        if hist.empty:
            print(f"  ⚠️  No price data for {ticker}")
            return result
        
        # Find contract date price (or closest trading day)
        contract_str = contract_date.strftime("%Y-%m-%d")
        
        # Get the close price on or near contract date
        if contract_str in hist.index.strftime("%Y-%m-%d").tolist():
            idx = hist.index.strftime("%Y-%m-%d").tolist().index(contract_str)
            contract_price = float(hist.iloc[idx]["Close"])
        else:
            # Find closest trading day
            hist_sorted = hist.sort_index()
            closest_idx = hist_sorted.index.get_indexer([contract_date], method='nearest')[0]
            contract_price = float(hist_sorted.iloc[closest_idx]["Close"])
        
        result["price_at_contract"] = Decimal(str(round(contract_price, 4)))
        
        # Calculate 7 days before
        before_7d = contract_date - timedelta(days=7)
        before_str = before_7d.strftime("%Y-%m-%d")
        
        for i, date_str in enumerate(hist.index.strftime("%Y-%m-%d")):
            if date_str <= before_str:
                before_price = float(hist.iloc[i]["Close"])
                pct_change = ((contract_price - before_price) / before_price) * 100
                result["price_before_7d"] = Decimal(str(round(pct_change, 2)))
        
        # Calculate after periods (1d, 7d)
        for days, key in [(1, "price_after_1d"), (7, "price_after_7d")]:
            after_date = contract_date + timedelta(days=days)
            after_str = after_date.strftime("%Y-%m-%d")
            
            for i, date_str in enumerate(hist.index.strftime("%Y-%m-%d")):
                if date_str >= after_str:
                    after_price = float(hist.iloc[i]["Close"])
                    pct_change = ((after_price - contract_price) / contract_price) * 100
                    result[key] = Decimal(str(round(pct_change, 2)))
                    break
        
        # Note: 1h and 6h changes require intraday data which is limited
        # For now, we estimate based on next day open vs close
        if len(hist) > 1:
            try:
                contract_idx = hist.index.strftime("%Y-%m-%d").tolist().index(
                    hist.index.strftime("%Y-%m-%d")[
                        hist.index.get_indexer([contract_date], method='nearest')[0]
                    ]
                )
                if contract_idx + 1 < len(hist):
                    next_day_open = float(hist.iloc[contract_idx + 1]["Open"])
                    next_day_high = float(hist.iloc[contract_idx + 1]["High"])
                    
                    # Estimate 1h as open movement, 6h as midpoint to high
                    result["price_after_1h"] = Decimal(str(round(
                        ((next_day_open - contract_price) / contract_price) * 100, 2
                    )))
                    midpoint = (next_day_open + next_day_high) / 2
                    result["price_after_6h"] = Decimal(str(round(
                        ((midpoint - contract_price) / contract_price) * 100, 2
                    )))
            except Exception:
                pass
        
        print(f"  📈 {ticker}: 7d before: {result['price_before_7d']}%, 1d after: {result['price_after_1d']}%")
        
    except Exception as e:
        print(f"  ⚠️  Price evolution error for {ticker}: {e}")
    
    return result


async def update_signal_prices(signal_id: int, ticker: str, contract_date: datetime, db) -> bool:
    """
    Update a signal with price evolution data.
    """
    from database import Signal
    from sqlalchemy import update
    
    prices = get_price_evolution(ticker, contract_date)
    
    if prices["price_at_contract"] is None:
        return False
    
    stmt = (
        update(Signal)
        .where(Signal.id == signal_id)
        .values(
            price_at_contract=prices["price_at_contract"],
            price_before_7d=prices["price_before_7d"],
            price_after_1h=prices["price_after_1h"],
            price_after_6h=prices["price_after_6h"],
            price_after_1d=prices["price_after_1d"],
            price_after_7d=prices["price_after_7d"],
        )
    )
    
    await db.execute(stmt)
    return True
