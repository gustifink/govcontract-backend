"""
Entity Resolution - Map company names to stock tickers

Uses RapidFuzz for fuzzy string matching + hardcoded mappings for known federal contractors.
"""
import re
from rapidfuzz import fuzz, process
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import Company
from config import get_settings

settings = get_settings()

# ============================================================================
# EXPLICIT FEDERAL CONTRACTOR MAPPINGS
# These are subsidiaries and trading names that don't fuzzy-match well
# ============================================================================
FEDERAL_CONTRACTOR_MAPPINGS = {
    # Aerospace & Defense
    "aerojet rocketdyne": "LHX",  # Acquired by L3Harris
    "aerojet rocketdyne of de": "LHX",
    "aerojet": "LHX",
    "l3harris": "LHX",
    "l3 harris": "LHX",
    
    # IT Services & Consulting
    "accenture federal services": "ACN",
    "accenture federal": "ACN",
    "cgi federal": "GIB",
    "general dynamics information technology": "GD",
    "gdit": "GD",
    "elsevier": "RELX",
    "deloitte consulting": None,  # Private
    "kpmg": None,  # Private
    "mckinsey": None,  # Private
    
    # Defense Contractors
    "booz allen hamilton": "BAH",
    "booz allen": "BAH",
    "parsons government services": "PSN",
    "parsons": "PSN",
    "mantech advanced systems": "MANT",
    "mantech": "MANT",
    "leidos": "LDOS",
    "peraton": None,  # Private (was PRTN but delisted)
    "peraton enterprise": None,
    
    # Energy & Environment  
    "ameresco": "AMRC",
    "ameresco inc": "AMRC",
    
    # Private Prisons / Gov Services
    "geo transport": "GEO",
    "geo group": "GEO",
    "geo reentry": "GEO",
    "corecivic": "CXW",
    
    # Healthcare
    "emergent biosolutions": "EBS",
    "siga technologies": "SIGA",
    
    # Other common contractors
    "raytheon": "RTX",
    "northrop grumman": "NOC",
    "lockheed martin": "LMT",
    "general dynamics": "GD",
    "boeing": "BA",
    "huntington ingalls": "HII",
    "science applications international": "SAIC",
    "saic": "SAIC",
    "caci international": "CACI",
    "caci nss": "CACI",
    "kratos defense": "KTOS",
    "kratos": "KTOS",
    "bwx technologies": "BWXT",
    "amentum": None,  # Private
    "amentum services": None,
}

# Common suffixes to strip from company names
COMPANY_SUFFIXES = [
    r"\binc\.?\b", r"\bincorporated\b", r"\bcorp\.?\b", r"\bcorporation\b",
    r"\bllc\b", r"\bllp\b", r"\blp\b", r"\bltd\.?\b", r"\blimited\b",
    r"\bco\.?\b", r"\bcompany\b", r"\bholding[s]?\b", r"\bgroup\b",
    r"\bplc\b", r"\bsa\b", r"\bnv\b", r"\bag\b", r"\bgmbh\b",
    r"\bthe\b", r"\b&\b", r"\band\b", r"\bof de\b", r"\bof\b"
]

SUFFIX_PATTERN = re.compile(
    "|".join(COMPANY_SUFFIXES), 
    re.IGNORECASE
)


def normalize_company_name(name: str) -> str:
    """
    Clean company name for matching.
    
    "KRATOS DEFENSE & SECURITY SOLUTIONS, INC." 
    -> "kratos defense security solutions"
    """
    # Lowercase
    name = name.lower()
    
    # Remove punctuation except spaces
    name = re.sub(r"[^\w\s]", " ", name)
    
    # Remove common suffixes
    name = SUFFIX_PATTERN.sub(" ", name)
    
    # Collapse multiple spaces
    name = re.sub(r"\s+", " ", name).strip()
    
    return name


class EntityResolver:
    """
    Resolves company names to stock tickers using:
    1. Explicit federal contractor mappings (highest priority)
    2. Exact match against company database
    3. Fuzzy matching as fallback
    """
    
    def __init__(self):
        self._company_cache: dict[str, tuple[str, str]] = {}  # normalized -> (ticker, name)
        self._loaded = False
    
    async def load_companies(self, db: AsyncSession) -> None:
        """Load all companies into memory for fast matching"""
        if self._loaded:
            return
            
        query = select(Company.ticker, Company.name, Company.name_normalized)
        result = await db.execute(query)
        
        for ticker, name, name_normalized in result.all():
            key = name_normalized or normalize_company_name(name)
            self._company_cache[key] = (ticker, name)
        
        self._loaded = True
        print(f"📊 Loaded {len(self._company_cache)} companies for matching")
    
    def match(self, awardee_name: str) -> tuple[str | None, str | None, float]:
        """
        Match an awardee name to a stock ticker.
        
        Returns:
            Tuple of (ticker, company_name, confidence_score) or (None, None, 0) if no match
        """
        normalized = normalize_company_name(awardee_name)
        
        # === PRIORITY 1: Check explicit federal contractor mappings ===
        for pattern, ticker in FEDERAL_CONTRACTOR_MAPPINGS.items():
            if pattern in normalized:
                if ticker is None:
                    # Explicitly marked as private/unmatchable
                    return None, None, 0.0
                # Get company name from cache if available
                for key, (t, name) in self._company_cache.items():
                    if t == ticker:
                        return ticker, name, 100.0
                return ticker, awardee_name, 100.0
        
        if not self._company_cache:
            return None, None, 0.0
        
        # === PRIORITY 2: Exact match ===
        if normalized in self._company_cache:
            ticker, name = self._company_cache[normalized]
            return ticker, name, 100.0
        
        # === PRIORITY 3: Fuzzy match ===
        choices = list(self._company_cache.keys())
        result = process.extractOne(
            normalized,
            choices,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=settings.fuzzy_match_threshold
        )
        
        if result:
            matched_name, score, _ = result
            ticker, name = self._company_cache[matched_name]
            return ticker, name, score
        
        return None, None, 0.0
    
    def clear_cache(self):
        """Clear the company cache (for reloading)"""
        self._company_cache.clear()
        self._loaded = False


# Singleton instance
entity_resolver = EntityResolver()
