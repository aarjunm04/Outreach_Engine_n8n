# src/sheet_sync.py

"""
Google Sheets Sync v2 (Apps Script WebApp)

- Loads final CSV from data/enriched_with_emails.csv
- Normalizes to the Google Sheet schema:
  NAME, EMAIL, ROLE, COMPANY, SOURCE, DATE, STATUS, TEMPLATE USED, NOTES
- Fetches existing rows from Apps Script GET endpoint
- Upserts by EMAIL:
    - New email  -> append full row (STATUS="Pending", TEMPLATE USED="")
    - Existing   -> update only backend-owned fields, keep STATUS / TEMPLATE USED
- Sends rows in batches to POST endpoint
"""

import json
import time
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
import requests
from loguru import logger

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"

INPUT_CSV = DATA_DIR / "enriched_with_emails.csv"  # pipeline output

BATCH_SIZE = 50
POST_TIMEOUT = 20
RETRIES = 3
RETRY_DELAY = 2

# Sheet header names (must match Google Sheet exactly)
SHEET_COLUMNS = [
    "NAME",
    "EMAIL",
    "ROLE",
    "COMPANY",
    "SOURCE",
    "DATE",
    "STATUS",
    "TEMPLATE USED",
    "NOTES",
]

# Columns owned by backend (safe to overwrite on resync)
BACKEND_OWNED = {"NAME", "EMAIL", "ROLE", "COMPANY", "SOURCE", "DATE", "NOTES"}

# Columns owned by outreach / Opal workflow (never overwrite here)
WORKFLOW_OWNED = {"STATUS", "TEMPLATE USED"}


class SheetSync:
    def __init__(self, webhook_post: str, webhook_get: Optional[str] = None) -> None:
        self.webhook_post = webhook_post
        self.webhook_get = webhook_get
        self.session = requests.Session()
        logger.info("[SheetSync] Initialized (POST webhook configured)")
        if self.webhook_get:
            logger.info("[SheetSync] GET webhook enabled for upsert/dedupe")

    # ------------------------------------------------------------------
    # Load local CSV and map to sheet schema
    # ------------------------------------------------------------------
    def _load_local_rows(self) -> pd.DataFrame:
        if not INPUT_CSV.exists():
            logger.error(f"[SheetSync] {INPUT_CSV} not found")
            return pd.DataFrame()

        df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
        logger.info(f"[SheetSync] Loaded {len(df)} rows from {INPUT_CSV}")

        # Expect columns from enrichment: first_name, last_name, job_title, company,
        # domain, linkedin_url, email, source, date (adjust if your CSV differs).
        # Map to the sheet schema.
        def build_name(row):
            first = str(row.get("first_name", "")).strip()
            last = str(row.get("last_name", "")).strip()
            if first or last:
                return f"{first} {last}".strip()
            return str(row.get("name", "")).strip()

        def build_notes(row):
            # Use LinkedIn URL as NOTES
            return str(row.get("linkedin_url", "")).strip()

        records: List[Dict[str, Any]] = []
        for _, r in df.iterrows():
            row = r.to_dict()

            rec: Dict[str, Any] = {}
            rec["NAME"] = build_name(row)
            rec["EMAIL"] = str(row.get("email", "")).strip()
            rec["ROLE"] = str(row.get("job_title", "")).strip()
            rec["COMPANY"] = str(row.get("company", "")).strip()
            rec["SOURCE"] = str(row.get("source", "Hunter.io")).strip() or "Hunter.io"
            rec["DATE"] = str(row.get("date", pd.Timestamp.utcnow().isoformat())).strip()
            rec["STATUS"] = "Pending"  # default for new leads
            rec["TEMPLATE USED"] = ""  # workflow will fill later
            rec["NOTES"] = build_notes(row)

            # Ensure all keys exist
            for col in SHEET_COLUMNS:
                rec.setdefault(col, "")

            records.append(rec)

        df_sheet = pd.DataFrame(records, columns=SHEET_COLUMNS).fillna("")
        logger.info(f"[SheetSync] Normalized {len(df_sheet)} rows to sheet schema")
        return df_sheet

    # ------------------------------------------------------------------
    # Fetch existing rows from Sheet
    # ------------------------------------------------------------------
    def _fetch_existing(self) -> pd.DataFrame:
        if not self.webhook_get:
            logger.warning("[SheetSync] No GET URL configured, skipping upsert")
            return pd.DataFrame()

        try:
            r = self.session.get(self.webhook_get, timeout=POST_TIMEOUT)
            if r.status_code != 200:
                logger.warning(f"[SheetSync] GET returned {r.status_code}")
                return pd.DataFrame()

            data = r.json()
            # Apps Script should return list of row dicts with same headers
            if isinstance(data, dict) and "data" in data:
                data = data["data"]

            df = pd.DataFrame(data, dtype=str).fillna("")
            logger.info(f"[SheetSync] Sheet currently has {len(df)} existing rows")
            return df
        except Exception as e:
            logger.error(f"[SheetSync] GET failed: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Upsert by EMAIL (preserve workflow-owned columns)
    # ------------------------------------------------------------------
    def _compute_changes(
        self, df_local: pd.DataFrame, df_sheet: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        # Build email → existing row map
        if df_sheet.empty:
            logger.info("[SheetSync] Sheet empty — all rows are new inserts")
            # All will be inserts; just return local rows
            return df_local.to_dict(orient="records")

        df_sheet["email_norm"] = (
            df_sheet.get("EMAIL", "").astype(str).str.strip().str.lower()
        )
        df_local["email_norm"] = (
            df_local.get("EMAIL", "").astype(str).str.strip().str.lower()
        )

        existing_by_email: Dict[str, Dict[str, Any]] = {}
        for _, r in df_sheet.iterrows():
            email = r.get("email_norm", "")
            if email:
                existing_by_email[email] = r.to_dict()

        changes: List[Dict[str, Any]] = []
        inserts, updates = 0, 0

        for _, r in df_local.iterrows():
            local = r.to_dict()
            email_norm = local.get("email_norm", "")
            if not email_norm:
                # No email → treat as pure insert
                changes.append({k: local.get(k, "") for k in SHEET_COLUMNS})
                inserts += 1
                continue

            if email_norm not in existing_by_email:
                # New email → insert full row
                changes.append({k: local.get(k, "") for k in SHEET_COLUMNS})
                inserts += 1
            else:
                # Existing email → upsert: keep workflow-owned fields from sheet
                existing = existing_by_email[email_norm]
                merged: Dict[str, Any] = {}
                for col in SHEET_COLUMNS:
                    if col in WORKFLOW_OWNED:
                        # Keep sheet value
                        merged[col] = existing.get(col, "")
                    elif col in BACKEND_OWNED:
                        # Overwrite with latest backend value
                        merged[col] = local.get(col, "")
                    else:
                        # Fallback: prefer local
                        merged[col] = local.get(col, existing.get(col, ""))
                changes.append(merged)
                updates += 1

        logger.info(
            f"[SheetSync] Upsert plan: {inserts} inserts, {updates} updates (by EMAIL)"
        )
        return changes

    # ------------------------------------------------------------------
    # POST batch to Apps Script
    # ------------------------------------------------------------------
    def _post_batch(self, batch: List[Dict[str, Any]]) -> bool:
        if not batch:
            return True

        payload = json.dumps({"data": batch})
        for attempt in range(1, RETRIES + 1):
            try:
                r = self.session.post(
                    self.webhook_post,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=POST_TIMEOUT,
                )
                logger.debug(
                    f"[SheetSync] POST attempt {attempt} → "
                    f"status={r.status_code} size={len(payload)}"
                )
                if r.status_code == 200:
                    body: Dict[str, Any] = {}
                    try:
                        body = r.json()
                    except Exception:
                        pass

                    if isinstance(body, dict) and body.get("status") == "error":
                        logger.warning(
                            f"[SheetSync] Apps Script logical error: {body.get('message')}"
                        )
                    else:
                        logger.info(f"[SheetSync] Batch ({len(batch)}) synced")
                        return True
            except Exception as e:
                logger.warning(f"[SheetSync] POST fail attempt {attempt}: {e}")
                time.sleep(RETRY_DELAY)

        logger.error("[SheetSync] Batch failed after retries")
        return False

    # ------------------------------------------------------------------
    # Public sync runner
    # ------------------------------------------------------------------
    def sync(self) -> int:
        logger.info("=" * 80)
        logger.info("🚀 [SheetSync] Starting sheet sync v2")
        logger.info("=" * 80)

        df_local = self._load_local_rows()
        if df_local.empty:
            logger.warning("[SheetSync] No local rows to sync")
            return 0

        df_sheet = self._fetch_existing()
        changes = self._compute_changes(df_local, df_sheet)

        if not changes:
            logger.info("[SheetSync] Nothing to push (no inserts/updates)")
            return 0

        sent = 0
        for i in range(0, len(changes), BATCH_SIZE):
            batch = changes[i : i + BATCH_SIZE]
            if self._post_batch(batch):
                sent += len(batch)

        logger.info(f"[SheetSync] Sync complete. Total rows sent: {sent}")
        return sent
