# ============================================================================
# src/enrichment.py - Production-Ready Email Enrichment with Hunter.io
# ============================================================================
"""
Hunter.io-based email enrichment engine with:
- Multi-key rotation (uses least-used key first)
- Monthly email cap (200 by default, configurable)
- Email confidence filtering (50%+ by default)
- Domain blacklist (gmail, yahoo, hotmail, etc.)
- Domain format validation (must have TLD like .com, .io)
- Last name validation (must be 2+ characters)
- Error handling and retry logic
- Auto-creates CSV files if missing
- CSV preservation (doesn't delete existing data)

Flow:
1. Load scraped profiles from data/scraper_output.csv
2. Initialize Hunter.io client with API key rotation
3. Validate names and domains BEFORE API calls
4. Find emails for each profile (respecting monthly cap)
5. Filter by confidence threshold (50%+)
6. Export enriched profiles to data/enriched_with_emails.csv

Cost: ~$0.005 per email found (~5 credits for 5 emails in test mode)
"""

import pandas as pd
import yaml
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from loguru import logger
import requests
import time

# ============================================================================
# Configuration Loading
# ============================================================================

def load_config() -> Dict[str, Any]:
    """Load settings.yaml configuration"""
    config_path = Path(__file__).parent.parent / "config" / "settings.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}


# ============================================================================
# Hunter.io API Client with Key Rotation
# ============================================================================

class HunterClient:
    """
    Hunter.io email finder API client with intelligent key rotation.
    
    Features:
    - Automatically rotates between multiple API keys
    - Uses least-used key first (preserves credits)
    - Respects rate limiting (retries on 429)
    - Validates domain format (must have TLD)
    - Validates last name (must be 2+ characters, not single letter)
    - Filters by confidence threshold
    - Skips blacklisted domains
    """
    
    BASE_URL = "https://api.hunter.io/v2"
    
    # Disposable email providers to skip
    DISPOSABLE_DOMAINS = {
        "wizard.com", "tempmail.com", "guerrillamail.com", "mailinator.com",
        "10minutemail.com", "throwaway.email", "fakeinbox.com", "temp-mail.org",
        "yopmail.com", "maildrop.cc", "trashmail.com", "sharklasers.com"
    }
    
    def __init__(self, api_keys: List[Dict[str, Any]], config: Dict[str, Any]):
        """
        Initialize Hunter.io client.
        
        Args:
            api_keys: List of {key: str, credits: int, status: str}
            config: Full settings.yaml config dict
        """
        self.api_keys = api_keys
        self.config = config
        self.email_confidence_threshold = config.get("enrichment", {}).get(
            "email_confidence_threshold", 50
        )
        self.blacklist_domains = set(
            config.get("enrichment", {}).get("blacklist_domains", [])
        )
        # Add disposable domains to blacklist
        self.blacklist_domains.update(self.DISPOSABLE_DOMAINS)
        
        self.request_timeout = config.get("enrichment", {}).get(
            "request_timeout", 30
        )
        
        logger.info(
            f"[Hunter] Initialized with {len(api_keys)} keys, "
            f"confidence threshold: {self.email_confidence_threshold}%"
        )
    
    def _validate_last_name(self, last_name: str) -> bool:
        """
        Validate last name for Hunter.io API.
        
        Hunter.io rejects: single letters (W., X., etc.)
        Valid: "Khanna", "Patel", "Agro"
        Invalid: "W.", "X.", single letter
        
        Args:
            last_name: Last name to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not last_name:
            return False
        
        # Remove dots/periods
        clean_name = last_name.replace(".", "").strip()
        
        # Must be 2+ characters minimum
        if len(clean_name) < 2:
            logger.debug(f"[Hunter] Invalid last name (too short): '{last_name}'")
            return False
        
        return True
    
    def _validate_domain(self, domain: str) -> bool:
        """
        Validate domain format for Hunter.io API.
        
        Valid: google.com, stripe.io, anthropic.com
        Invalid: ai, google, company (no TLD), None, empty
        
        Args:
            domain: Domain string to validate
            
        Returns:
            True if valid domain format, False otherwise
        """
        if not domain:
            return False
        
        domain = domain.lower().strip()
        
        # Must have at least one dot (TLD separator)
        if "." not in domain:
            logger.debug(f"[Hunter] Invalid domain (no TLD): {domain}")
            return False
        
        # Split and validate
        parts = domain.split(".")
        if len(parts) < 2:
            return False
        
        # TLD must be 2+ characters (com, io, co, etc.)
        tld = parts[-1]
        if len(tld) < 2 or not tld.isalpha():
            logger.debug(f"[Hunter] Invalid TLD: {tld}")
            return False
        
        # Domain part must not be empty
        if not parts[0]:
            return False
        
        return True
    
    def _get_active_key(self) -> Optional[Tuple[str, int]]:
        """
        Get the least-used active API key.
        
        Returns:
            Tuple of (api_key, key_index) or None if no active keys
        """
        active_keys = [
            (k["key"], i, k.get("credits", 0))
            for i, k in enumerate(self.api_keys)
            if k.get("status") == "active"
        ]
        
        if not active_keys:
            logger.error("[Hunter] No active API keys configured")
            return None
        
        # Sort by credits (least used first) then by index
        active_keys.sort(key=lambda x: (-x[2], x[1]))
        key, key_index, credits = active_keys[0]
        
        logger.debug(f"[Hunter] Using key {key_index + 1} (credits: {credits})")
        return key, key_index
    
    def find_email(
        self, first_name: str, last_name: str, domain: str
    ) -> Optional[Dict[str, Any]]:
        """
        Find email for person at domain using Hunter.io API.
        
        Args:
            first_name: Person's first name
            last_name: Person's last name
            domain: Company domain (e.g., 'example.com')
        
        Returns:
            {
                "email": "john@example.com",
                "confidence": 95,
                "found": True
            }
            or None if not found/error
        """
        
        if not domain:
            logger.debug("[Hunter] No domain provided")
            return None
        
        # VALIDATE DOMAIN FORMAT FIRST
        if not self._validate_domain(domain):
            logger.debug(f"[Hunter] Skipping invalid domain format: {domain}")
            return None
        
        # VALIDATE LAST NAME
        if not self._validate_last_name(last_name):
            logger.debug(
                f"[Hunter] Skipping - invalid last name: '{last_name}'"
            )
            return None
        
        # Check blacklist (skip gmail, yahoo, disposable, etc.)
        domain_lower = domain.lower()
        if any(domain_lower.endswith(bd) for bd in self.blacklist_domains):
            logger.debug(f"[Hunter] Skipping blacklisted domain: {domain}")
            return None
        
        # Get active key
        key_info = self._get_active_key()
        if not key_info:
            return None
        
        api_key, key_index = key_info
        
        # Build request parameters
        params = {
            "api_key": api_key,
            "domain": domain,
            "first_name": first_name,
            "last_name": last_name,
        }
        
        # LOG BEFORE API CALL
        logger.info(
            f"[Hunter] Searching: {first_name} {last_name} @ {domain}"
        )
        
        try:
            resp = requests.get(
                f"{self.BASE_URL}/email-finder",
                params=params,
                timeout=self.request_timeout
            )
            
            if resp.status_code == 429:  # Rate limited
                logger.warning("[Hunter] Rate limited, waiting 60s...")
                time.sleep(60)
                return self.find_email(first_name, last_name, domain)  # Retry
            
            if resp.status_code == 400:
                logger.error(
                    f"[Hunter] ❌ 400 Bad Request for {domain}: "
                    f"{resp.text[:300]}"
                )
                return None
            
            if resp.status_code not in (200, 201):
                logger.warning(
                    f"[Hunter] API error {resp.status_code}: {resp.text[:200]}"
                )
                return None
            
            data = resp.json()
            
            # Check if email found in response
            if not data.get("data"):
                logger.debug(f"[Hunter] No email found in Hunter response")
                return {"found": False}
            
            email = data["data"].get("email")
            # FIX: Hunter.io returns "score" not "confidence"!
            score = data["data"].get("score", 0)
            
            if not email:
                logger.debug(f"[Hunter] No email in response data")
                return {"found": False}
            
            # Check confidence threshold
            if score < self.email_confidence_threshold:
                logger.debug(
                    f"[Hunter] {email} below threshold "
                    f"({score}% < {self.email_confidence_threshold}%)"
                )
                return {
                    "found": False,
                    "email": email,
                    "confidence": score
                }
            
            # Success!
            logger.info(
                f"[Hunter] ✅ Found: {email} ({score}%)"
            )
            
            # Update key credits (from API response)
            if "data" in data and "emails_remaining" in data["data"]:
                remaining = data["data"]["emails_remaining"]
                self.api_keys[key_index]["credits"] = remaining
                logger.debug(
                    f"[Hunter] Key {key_index + 1} remaining: {remaining}"
                )
            
            return {
                "email": email,
                "confidence": score,
                "found": True
            }
        
        except requests.exceptions.Timeout:
            logger.warning("[Hunter] Request timeout")
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"[Hunter] Request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"[Hunter] Unexpected error: {e}")
            return None


# ============================================================================
# Main Enrichment Engine
# ============================================================================

def run_enrichment() -> pd.DataFrame:
    """
    Main enrichment pipeline:
    
    Stage 1: Load scraped profiles from CSV
    Stage 2: Initialize Hunter.io client with API key rotation
    Stage 3: Find emails for each profile (respecting monthly cap)
    Stage 4: Filter by confidence and export enriched profiles
    
    Returns:
        DataFrame with enriched profiles (only rows with emails)
    """
    
    config = load_config()
    
    logger.info("=" * 80)
    logger.info("📧 OUTREACH ENGINE - ENRICHMENT MODULE")
    logger.info("=" * 80)
    logger.info("")
    
    # ========================================================================
    # STAGE 1: LOAD SCRAPED PROFILES
    # ========================================================================
    
    logger.info("📥 STAGE 1: LOAD SCRAPED PROFILES")
    logger.info("-" * 80)
    
    input_csv = "data/scraper_output.csv"
    input_path = Path(input_csv)
    
    # Check if scraper output exists
    if not input_path.exists():
        logger.error(f"❌ Input CSV not found: {input_csv}")
        logger.error("   Make sure scraper.py ran first")
        logger.error("   Run: python -m src.main")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(input_csv)
        logger.info(f"✅ Loaded {len(df)} profiles from {input_csv}")
    except Exception as e:
        logger.error(f"❌ Failed to load CSV: {e}")
        return pd.DataFrame()
    
    if df.empty:
        logger.error("❌ Scraped CSV is empty (scraper found no profiles)")
        return df
    
    logger.info(f"   Columns: {', '.join(df.columns)}")
    logger.info("")
    
    # ========================================================================
    # STAGE 2: INITIALIZE HUNTER.IO CLIENT
    # ========================================================================
    
    logger.info("🔑 STAGE 2: INITIALIZE HUNTER.IO CLIENT")
    logger.info("-" * 80)
    
    api_keys_config = config.get("enrichment", {}).get("api_keys", [])
    
    if not api_keys_config:
        logger.error("❌ No Hunter.io API keys configured in settings.yaml")
        logger.error("   Add keys under enrichment.api_keys")
        return pd.DataFrame()
    
    # Log API keys status
    active_count = sum(
        1 for k in api_keys_config if k.get("status") == "active"
    )
    logger.info(f"API Keys: {len(api_keys_config)} total, {active_count} active")
    for i, key_config in enumerate(api_keys_config, 1):
        status = key_config.get("status", "unknown")
        credits = key_config.get("credits", "?")
        key_preview = key_config.get("key", "")[:10] + "***"
        logger.info(f"  Key {i}: {status} ({credits} credits) - {key_preview}")
    
    # Initialize Hunter.io client
    client = HunterClient(api_keys_config, config)
    logger.info("")
    
    # ========================================================================
    # STAGE 3: ENRICH WITH EMAILS
    # ========================================================================
    
    logger.info("📧 STAGE 3: FIND EMAILS VIA HUNTER.IO")
    logger.info("-" * 80)
    
    email_cap = config.get("enrichment", {}).get(
        "monthly_email_cap", 200
    )
    logger.info(f"Email cap: {email_cap}")
    logger.info(f"Confidence threshold: {client.email_confidence_threshold}%")
    logger.info("")
    
    # Add email columns if not present
    if "email" not in df.columns:
        df["email"] = ""
    if "confidence" not in df.columns:
        df["confidence"] = 0
    
    enriched_count = 0
    skipped_count = 0
    invalid_domain_count = 0
    invalid_name_count = 0
    
    for idx, row in df.iterrows():
        # Check if already enriched
        if pd.notna(row.get("email")) and str(row["email"]).strip():
            logger.debug(f"  [{idx + 1}] Already has email: {row['email']}")
            enriched_count += 1
            continue
        
        # Check email cap (stop if reached)
        if enriched_count >= email_cap:
            logger.info(
                f"✅ Reached email cap ({email_cap}). Stopping enrichment."
            )
            break
        
        # Extract required fields
        first_name = str(row.get("first_name", "")).strip()
        last_name = str(row.get("last_name", "")).strip()
        domain = str(row.get("domain", "")).strip()
        
        # Validate required fields
        if not (first_name and last_name):
            logger.debug(
                f"  [{idx + 1}] Skipping - missing name "
                f"(name: {first_name} {last_name})"
            )
            skipped_count += 1
            continue
        
        if not domain:
            logger.debug(f"  [{idx + 1}] Skipping - missing domain")
            skipped_count += 1
            continue
        
        # Validate domain format
        if not client._validate_domain(domain):
            logger.debug(
                f"  [{idx + 1}] Skipping - invalid domain format: {domain}"
            )
            invalid_domain_count += 1
            continue
        
        # Validate last name (Hunter.io requirement)
        if not client._validate_last_name(last_name):
            logger.debug(
                f"  [{idx + 1}] Skipping - invalid last name: '{last_name}'"
            )
            invalid_name_count += 1
            continue
        
        # Call Hunter.io API
        result = client.find_email(first_name, last_name, domain)
        
        if result and result.get("found"):
            email = result["email"]
            confidence = result["confidence"]
            df.at[idx, "email"] = email
            df.at[idx, "confidence"] = confidence
            enriched_count += 1
            logger.info(
                f"  ✅ [{idx + 1}] {first_name} {last_name} → "
                f"{email} ({confidence}%)"
            )
        else:
            logger.debug(
                f"  ❌ [{idx + 1}] {first_name} {last_name} @ {domain} "
                f"- No email found"
            )
            skipped_count += 1
        
        # Small delay between requests (avoid rate limiting)
        time.sleep(0.5)
    
    logger.info("")
    logger.info(
        f"✅ Enrichment complete: {enriched_count} emails found, "
        f"{skipped_count} not found, {invalid_domain_count} invalid domains, "
        f"{invalid_name_count} invalid names"
    )
    logger.info("")
    
    # ========================================================================
    # STAGE 4: FILTER & EXPORT
    # ========================================================================
    
    logger.info("💾 STAGE 4: EXPORT ENRICHED PROFILES")
    logger.info("-" * 80)
    
    # Keep only rows with valid emails
    df_with_emails = df[
        df["email"].notna() & (df["email"].astype(str).str.strip() != "")
    ].copy()
    
    # Output path
    output_csv = Path(
        config.get("enrichment", {}).get(
            "output_csv", "data/enriched_with_emails.csv"
        )
    )
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    
    # Export
    df_with_emails.to_csv(output_csv, index=False)
    logger.info(
        f"✅ Exported {len(df_with_emails)} enriched profiles to {output_csv}"
    )
    
    if len(df_with_emails) == 0:
        logger.warning("⚠️  No emails found. Sheet sync will skip.")
    
    logger.info("")
    logger.info("=" * 80)
    logger.info(
        f"✅ ENRICHMENT COMPLETE: {len(df_with_emails)} profiles with emails"
    )
    logger.info("=" * 80)
    
    return df_with_emails