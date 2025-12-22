"""
USASpending.gov API Ingestion Pipeline - TRANSACTION LEVEL

Fetches RECENT contract TRANSACTIONS (modifications/payments) from USASpending.gov API.
This shows what was JUST paid, not lifetime contract totals.

No API key required - fully open API!
"""
import httpx
from datetime import datetime, timedelta
from typing import Any
from config import get_settings

settings = get_settings()

# Transaction search endpoint
USASPENDING_TRANSACTION_URL = "https://api.usaspending.gov/api/v2/search/spending_by_transaction/"

# Award type codes for contracts only (not grants, loans, etc.)
CONTRACT_AWARD_TYPES = ["A", "B", "C", "D"]

# Fields to retrieve from USASpending transactions
FIELDS_TO_RETRIEVE = [
    "Recipient Name",
    "Award ID", 
    "Mod",
    "Action Date",
    "Transaction Amount",
    "Awarding Agency",
    "Awarding Sub Agency",
    "Action Type",
    "Transaction Description",
    "generated_internal_id"
]


async def fetch_contract_awards(days_back: int = 3) -> list[dict[str, Any]]:
    """
    Fetch recent contract TRANSACTIONS from USASpending.gov API.
    
    This returns individual contract modifications/payments, not aggregate totals.
    This is what competitors like Quiver show - the "last contract paid" amount.
    
    Args:
        days_back: How far back to search (default 3 days for recent transactions)
        
    Returns:
        List of transaction dictionaries
    """
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Build the request payload for TRANSACTION-level search
    payload = {
        "filters": {
            "time_period": [
                {
                    "start_date": start_date,
                    "end_date": end_date
                }
            ],
            "award_type_codes": CONTRACT_AWARD_TYPES
        },
        "fields": FIELDS_TO_RETRIEVE,
        "limit": 100,
        "page": 1,
        "sort": "Transaction Amount",
        "order": "desc"
    }
    
    all_transactions = []
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            # Fetch multiple pages
            for page in range(1, 6):  # Max 5 pages = 500 transactions
                payload["page"] = page
                
                print(f"  📡 Fetching transactions page {page} from USASpending.gov...")
                
                response = await client.post(
                    USASPENDING_TRANSACTION_URL,
                    json=payload
                )
                response.raise_for_status()
                data = response.json()
                
                results = data.get("results", [])
                if not results:
                    break
                
                # Filter by minimum amount here (after fetch)
                for r in results:
                    amount = r.get("Transaction Amount") or 0
                    if amount >= settings.min_award_amount:
                        all_transactions.append(r)
                
                print(f"    → Got {len(results)} transactions, {len([r for r in results if (r.get('Transaction Amount') or 0) >= settings.min_award_amount])} above ${settings.min_award_amount/1e6:.0f}M threshold")
                
                # Check if there are more pages
                if len(results) < 100:
                    break
                    
        except httpx.HTTPError as e:
            print(f"⚠️  USASpending API error: {e}")
            return []
    
    print(f"📥 Fetched {len(all_transactions)} transactions from USASpending.gov (last {days_back} days)")
    return all_transactions


def parse_contract(raw: dict[str, Any]) -> dict[str, Any] | None:
    """
    Parse raw USASpending TRANSACTION data into normalized format.
    
    Returns None if transaction should be filtered (e.g., missing required fields).
    """
    # Extract recipient name
    awardee_name = raw.get("Recipient Name")
    
    if not awardee_name:
        return None
    
    # Extract transaction amount (this is the key difference!)
    award_amount = raw.get("Transaction Amount") or 0
    
    # Handle string amounts (sometimes returned with formatting)
    if isinstance(award_amount, str):
        award_amount = float(award_amount.replace(",", "").replace("$", ""))
    
    # Filter out small transactions
    if award_amount < settings.min_award_amount:
        return None
    
    # Generate unique contract ID including modification number
    award_id = raw.get("Award ID") or ""
    mod_number = raw.get("Mod") or "0"
    action_date = raw.get("Action Date") or ""
    
    # Make ID unique to this specific transaction
    contract_id = raw.get("generated_internal_id") or f"{award_id}_{mod_number}_{action_date}"
    if not contract_id:
        return None
    
    # Add modification to contract ID to ensure uniqueness per transaction
    contract_id = f"{contract_id}_MOD{mod_number}"
    
    # Build agency name
    agency = raw.get("Awarding Agency", "")
    sub_agency = raw.get("Awarding Sub Agency", "")
    agency_name = f"{agency}" + (f" - {sub_agency}" if sub_agency and sub_agency != agency else "")
    
    # Parse action date
    contract_date = None
    if action_date:
        try:
            contract_date = datetime.strptime(action_date, "%Y-%m-%d")
        except ValueError:
            pass
    
    # Build USASpending URL for the contract
    internal_id = raw.get("generated_internal_id", "")
    usa_spending_url = f"https://www.usaspending.gov/award/{internal_id}" if internal_id else None
    
    # Get action type description
    action_type_codes = {
        "A": "New",
        "B": "Continuation", 
        "C": "Modification",
        "D": "Deletion",
        "G": "Grant"
    }
    action_type = raw.get("Action Type", "")
    action_type_desc = action_type_codes.get(action_type, action_type)
    
    # Build normalized contract
    return {
        "contract_id": str(contract_id),
        "awardee_name": awardee_name,
        "agency_name": agency_name or "Unknown Agency",
        "action_type": action_type_desc,
        "description": raw.get("Transaction Description", ""),
        "award_amount": float(award_amount),
        "potential_ceiling": None,  # Transaction doesn't have ceiling
        "contract_date": contract_date,
        "sam_gov_url": usa_spending_url  # Keep field name for compatibility
    }
