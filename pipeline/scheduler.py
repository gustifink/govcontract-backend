"""
Pipeline Scheduler - Orchestrates the full ETL pipeline

Runs on a CRON schedule (default: every 30 minutes).
"""
from datetime import datetime
from typing import Any
from decimal import Decimal
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert

from database import AsyncSessionLocal, Signal, Company
from pipeline.ingestion import fetch_contract_awards, parse_contract
from pipeline.entity_resolution import entity_resolver
from pipeline.valuation import score_signal
from config import get_settings

settings = get_settings()

# Pipeline state
_scheduler: AsyncIOScheduler | None = None
_last_run: datetime | None = None
_last_result: dict[str, Any] = {}


async def run_pipeline() -> dict[str, Any]:
    """
    Execute the full data pipeline:
    1. Fetch contracts from SAM.gov
    2. Resolve entities to tickers
    3. Score and filter
    4. Store qualifying signals
    """
    global _last_run, _last_result
    
    start_time = datetime.now()
    stats = {
        "status": "running",
        "contracts_fetched": 0,
        "contracts_parsed": 0,
        "entities_matched": 0,
        "signals_created": 0,
        "errors": []
    }
    
    try:
        async with AsyncSessionLocal() as db:
            # Load company data for entity resolution
            await entity_resolver.load_companies(db)
            
            # Step 1: Fetch contracts
            print("\n🔄 Starting pipeline run...")
            raw_contracts = await fetch_contract_awards(days_back=7)
            stats["contracts_fetched"] = len(raw_contracts)
            
            # Step 2: Parse and filter
            parsed_contracts = []
            for raw in raw_contracts:
                parsed = parse_contract(raw)
                if parsed:
                    parsed_contracts.append(parsed)
            
            stats["contracts_parsed"] = len(parsed_contracts)
            print(f"📝 Parsed {len(parsed_contracts)} valid contracts")
            
            # Step 3 & 4: Match, score, and store
            for contract in parsed_contracts:
                try:
                    # Entity resolution
                    ticker, company_name, confidence = entity_resolver.match(contract["awardee_name"])
                    
                    if not ticker:
                        continue
                    
                    stats["entities_matched"] += 1
                    print(f"  ✓ Matched: {contract['awardee_name'][:40]}... → {ticker} ({confidence:.0f}%)")
                    
                    # Score the signal
                    score_result = score_signal(
                        ticker=ticker,
                        award_amount=contract["award_amount"],
                        potential_ceiling=contract.get("potential_ceiling")
                    )
                    
                    if not score_result:
                        continue
                    
                    # Fetch price evolution data
                    price_data = {
                        'price_at_contract': None,
                        'price_before_1h': None,
                        'price_before_6h': None,
                        'price_before_24h': None,
                        'price_after_1m': None,
                        'price_after_1h': None,
                        'price_after_6h': None,
                        'price_after_24h': None,
                    }
                    
                    try:
                        import yfinance as yf
                        import pandas as pd
                        
                        stock = yf.Ticker(ticker)
                        contract_date = contract.get("contract_date")
                        
                        # Convert contract_date to datetime if string
                        if isinstance(contract_date, str):
                            contract_date = datetime.fromisoformat(contract_date.replace('Z', '+00:00'))
                        
                        # For intraday data: use 1m interval for last 7 days
                        # yfinance limits: 1m data only available for last 7 days
                        hist_1m = stock.history(period="7d", interval="1m")
                        hist_1h = stock.history(period="60d", interval="1h")
                        hist_1d = stock.history(period="60d", interval="1d")
                        
                        if not hist_1d.empty:
                            # Get price at contract date (use most recent close as proxy)
                            price_at_contract = float(hist_1d['Close'].iloc[-1])
                            price_data['price_at_contract'] = price_at_contract
                            
                            # Calculate BEFORE changes (price went from X to announcement price)
                            # -24h: % change from 24h before to announcement
                            if len(hist_1d) > 1:
                                price_24h_before = float(hist_1d['Close'].iloc[-2])  # Previous day close
                                price_data['price_before_24h'] = round(((price_at_contract - price_24h_before) / price_24h_before) * 100, 2)
                            
                            # -6h and -1h from intraday data
                            if not hist_1h.empty and len(hist_1h) > 6:
                                price_6h_before = float(hist_1h['Close'].iloc[-7])
                                price_data['price_before_6h'] = round(((price_at_contract - price_6h_before) / price_6h_before) * 100, 2)
                            if not hist_1h.empty and len(hist_1h) > 1:
                                price_1h_before = float(hist_1h['Close'].iloc[-2])
                                price_data['price_before_1h'] = round(((price_at_contract - price_1h_before) / price_1h_before) * 100, 2)
                            
                            # AFTER changes - For recent contracts we simulate with recent data
                            # In production, would need to wait for actual post-announcement prices
                            # For now, use intraday volatility as proxy
                            if not hist_1m.empty and len(hist_1m) > 1:
                                # Simulate +1m as small movement
                                price_data['price_after_1m'] = round((float(hist_1m['Close'].iloc[-1]) - float(hist_1m['Open'].iloc[-1])) / float(hist_1m['Open'].iloc[-1]) * 100, 2)
                            
                            if not hist_1h.empty and len(hist_1h) > 1:
                                last_close = float(hist_1h['Close'].iloc[-1])
                                prev_close = float(hist_1h['Close'].iloc[-2])
                                price_data['price_after_1h'] = round(((last_close - prev_close) / prev_close) * 100, 2)
                            
                            if not hist_1h.empty and len(hist_1h) > 6:
                                last_close = float(hist_1h['Close'].iloc[-1])
                                six_ago = float(hist_1h['Close'].iloc[-7])
                                price_data['price_after_6h'] = round(((last_close - six_ago) / six_ago) * 100, 2)
                            
                            if len(hist_1d) > 1:
                                last_close = float(hist_1d['Close'].iloc[-1])
                                prev_day = float(hist_1d['Close'].iloc[-2])
                                price_data['price_after_24h'] = round(((last_close - prev_day) / prev_day) * 100, 2)
                                
                    except Exception as e:
                        print(f"  ⚠️  Could not fetch price evolution for {ticker}: {e}")
                    
                    # Upsert signal (avoid duplicates)
                    stmt = insert(Signal).values(
                        contract_id=contract["contract_id"],
                        ticker=ticker,
                        agency_name=contract["agency_name"],
                        contract_description=contract["description"],
                        award_amount=contract["award_amount"],
                        potential_ceiling=contract.get("potential_ceiling"),
                        market_cap_at_time=score_result["market_cap"],
                        impact_ratio=score_result["impact_ratio"],
                        contract_date=contract.get("contract_date"),
                        sam_gov_url=contract.get("sam_gov_url"),
                        detected_at=datetime.utcnow(),
                        price_at_contract=price_data['price_at_contract'],
                        price_before_1h=price_data['price_before_1h'],
                        price_before_6h=price_data['price_before_6h'],
                        price_before_24h=price_data['price_before_24h'],
                        price_after_1m=price_data['price_after_1m'],
                        price_after_1h=price_data['price_after_1h'],
                        price_after_6h=price_data['price_after_6h'],
                        price_after_24h=price_data['price_after_24h'],
                    ).on_conflict_do_nothing(index_elements=["contract_id"])
                    
                    result = await db.execute(stmt)
                    
                    if result.rowcount > 0:
                        stats["signals_created"] += 1
                        price_str = f" @ ${price_at_contract:.2f}" if price_at_contract else ""
                        print(f"  💎 NEW SIGNAL: ${ticker}{price_str} - ${contract['award_amount']/1e6:.1f}M ({score_result['impact_ratio']}% impact)")
                    
                except Exception as e:
                    stats["errors"].append(str(e))
                    print(f"  ⚠️  Error processing contract: {e}")
            
            await db.commit()
        
        stats["status"] = "completed"
        print(f"\n✅ Pipeline completed: {stats['signals_created']} new signals from {stats['contracts_fetched']} contracts")
        
    except Exception as e:
        stats["status"] = "error"
        stats["errors"].append(str(e))
        print(f"❌ Pipeline error: {e}")
    
    _last_run = start_time
    _last_result = stats
    
    return stats


async def run_pipeline_now() -> dict[str, Any]:
    """Manually trigger pipeline (for API endpoint)"""
    return await run_pipeline()


def start_scheduler():
    """Start the APScheduler for periodic pipeline runs"""
    global _scheduler
    
    _scheduler = AsyncIOScheduler()
    _scheduler.add_job(
        run_pipeline,
        "interval",
        minutes=settings.pipeline_interval_minutes,
        id="contract_pipeline",
        replace_existing=True
    )
    _scheduler.start()


def get_pipeline_status() -> dict[str, Any]:
    """Get current pipeline status"""
    next_run = None
    if _scheduler:
        job = _scheduler.get_job("contract_pipeline")
        if job and job.next_run_time:
            next_run = job.next_run_time.isoformat()
    
    return {
        "status": _last_result.get("status", "idle"),
        "last_run": _last_run.isoformat() if _last_run else None,
        "next_run": next_run,
        **_last_result
    }
