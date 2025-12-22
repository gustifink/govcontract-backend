"""
Seed Companies Table

Downloads NYSE/NASDAQ company list and populates the companies table.
Can be run standalone or imported.
"""
import asyncio
import yfinance as yf
import pandas as pd
from sqlalchemy.dialects.sqlite import insert

from database import AsyncSessionLocal, Company
from pipeline.entity_resolution import normalize_company_name

# Comprehensive list of government contractors and publicly traded companies
# that regularly receive federal contracts
SEED_TICKERS = [
    # === Major Defense Primes ===
    "LMT",   # Lockheed Martin
    "RTX",   # Raytheon
    "NOC",   # Northrop Grumman
    "GD",    # General Dynamics
    "BA",    # Boeing
    "LHX",   # L3Harris Technologies
    "HII",   # Huntington Ingalls
    
    # === Defense IT & Services ===
    "LDOS",  # Leidos
    "SAIC",  # SAIC
    "BAH",   # Booz Allen Hamilton
    "CACI",  # CACI International
    "PSN",   # Parsons Corporation
    "KBR",   # KBR Inc
    "MANT",  # ManTech International
    
    # === Aerospace ===
    "AJRD",  # Aerojet Rocketdyne (now part of LHX but may have separate contracts)
    "RKLB",  # Rocket Lab
    "KTOS",  # Kratos Defense
    "AVAV",  # AeroVironment
    "TDG",   # TransDigm
    "HEI",   # HEICO
    "HXL",   # Hexcel
    "TXT",   # Textron
    "CW",    # Curtiss-Wright
    "SPR",   # Spirit AeroSystems
    
    # === Cybersecurity ===
    "CRWD",  # CrowdStrike
    "PANW",  # Palo Alto Networks
    "FTNT",  # Fortinet
    "ZS",    # Zscaler
    "NET",   # Cloudflare
    "S",     # SentinelOne
    "OKTA",  # Okta
    "TENB",  # Tenable
    "QLYS",  # Qualys
    "RPD",   # Rapid7
    
    # === IT Services & Consulting ===
    "ACN",   # Accenture
    "IBM",   # IBM
    "ORCL",  # Oracle
    "MSFT",  # Microsoft
    "GOOGL", # Google
    "AMZN",  # Amazon (AWS)
    "CTSH",  # Cognizant
    "INFY",  # Infosys
    "WIT",   # Wipro
    "GIB",   # CGI Group
    "DXC",   # DXC Technology
    "EPAM",  # EPAM Systems
    
    # === Palantir & Analytics ===
    "PLTR",  # Palantir
    "AIT",   # Applied Industrial Tech
    
    # === Healthcare & Pharma (Gov contracts) ===
    "EBS",   # Emergent BioSolutions
    "MRNA",  # Moderna
    "NVAX",  # Novavax
    "SIGA",  # SIGA Technologies
    "UNH",   # UnitedHealth
    "CI",    # Cigna
    "HUM",   # Humana
    "CVS",   # CVS Health
    
    # === Nuclear & Energy ===
    "BWXT",  # BWX Technologies
    "CEG",   # Constellation Energy
    "NEE",   # NextEra Energy
    "DUK",   # Duke Energy
    
    # === Specialty Defense ===
    "MRCY",  # Mercury Systems
    "AXON",  # Axon (Tasers, body cams)
    "OSIS",  # OSI Systems
    "FLIR",  # FLIR (now part of Teledyne)
    "TDY",   # Teledyne
    
    # === Private Prisons / Gov Services ===
    "GEO",   # GEO Group
    "CXW",   # CoreCivic
    
    # === Construction & Engineering ===
    "FLR",   # Fluor
    "J",     # Jacobs Engineering
    "PWR",   # Quanta Services
    "ACM",   # AECOM
    
    # === Telecom & Communications ===
    "T",     # AT&T
    "VZ",    # Verizon
    "TMUS",  # T-Mobile
    
    # === Shipbuilding ===
    "ASGN",  # ASGN Inc
    "GDYN",  # Grid Dynamics
    
    # === Large Cap Tech (for filtering) ===
    "AAPL", "NVDA", "TSLA", "META"
]


async def seed_companies(tickers: list[str] = None):
    """
    Fetch company data from yfinance and insert into database.
    """
    tickers = tickers or SEED_TICKERS
    
    print(f"🌱 Seeding {len(tickers)} companies...")
    
    async with AsyncSessionLocal() as db:
        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                
                name = info.get("longName") or info.get("shortName") or ticker
                
                stmt = insert(Company).values(
                    ticker=ticker,
                    name=name,
                    name_normalized=normalize_company_name(name),
                    market_cap=info.get("marketCap"),
                    avg_volume=info.get("averageVolume"),
                    sector=info.get("sector")
                ).on_conflict_do_update(
                    index_elements=["ticker"],
                    set_={
                        "name": name,
                        "name_normalized": normalize_company_name(name),
                        "market_cap": info.get("marketCap"),
                        "avg_volume": info.get("averageVolume"),
                        "sector": info.get("sector")
                    }
                )
                
                await db.execute(stmt)
                print(f"  ✓ {ticker}: {name[:40]}...")
                
            except Exception as e:
                print(f"  ⚠️  {ticker}: {e}")
        
        await db.commit()
    
    print("✅ Company seeding complete")


if __name__ == "__main__":
    asyncio.run(seed_companies())
