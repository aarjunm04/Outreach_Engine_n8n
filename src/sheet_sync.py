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
from typing import Dict, Any, List

import pandas as pd
import requests
from loguru import logger

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"

INPUT_CSV = DATA_DIR / "enriched_with_emails.csv"  # default pipeline output

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
    def __init__(self, webhook_url: str) -> None:
        self.webhook_url = webhook_url
        self.session = requests.Session()
        logger.info("[SheetSync] Initialized (single webhook_url for POST/GET)")

    # ------------------------------------------------------------------
    # Load local CSV and map to sheet schema
    # ------------------------------------------------------------------
    def _load_local_rows(self, csv_path: Path | None = None) -> pd.DataFrame:
        path = csv_path or INPUT_CSV
        if not path.exists():
            logger.error(f"[SheetSync] {path} not found")
            return pd.DataFrame()

        df = pd.read_csv(path, dtype=str).fillna("")
        logger.info(f"[SheetSync] Loaded {len(df)} rows from {path}")

        def build_name(row: Dict[str, Any]) -> str:
            first = str(row.get("first_name", "")).strip()
            last = str(row.get("last_name", "")).strip()
            if first or last:
                return f"{first} {last}".strip()
            return str(row.get("name", "")).strip()

        def build_notes(row: Dict[str, Any]) -> str:
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
            rec["DATE"] = str(
                row.get("date", pd.Timestamp.utcnow().isoformat())
            ).strip()
            rec["STATUS"] = "Pending"  # default for new leads
            rec["TEMPLATE USED"] = ""  # workflow will fill later
            rec["NOTES"] = build_notes(row)

            for col in SHEET_COLUMNS:
                rec.setdefault(col, "")

            records.append(rec)

        df_sheet = pd.DataFrame(records, columns=SHEET_COLUMNS).fillna("")
        logger.info(f"[SheetSync] Normalized {len(df_sheet)} rows to sheet schema")
        return df_sheet

    # ------------------------------------------------------------------
    # Fetch existing rows from Sheet (single webhook_url for GET)
    # ------------------------------------------------------------------
    def _fetch_existing(self) -> pd.DataFrame:
        """Fetch existing rows from Google Sheets via the same webhook URL.

        Apps Script doGet(e) should return:
        { "status": "ok", "data": [ { "NAME": "...", "EMAIL": "...", ... }, ... ] }
        """
        if not self.webhook_url:
            logger.warning(
                "[SheetSync] No webhook_url configured, skipping existing-row fetch"
            )
            return pd.DataFrame()

        try:
            r = self.session.get(self.webhook_url, timeout=POST_TIMEOUT)
            if r.status_code != 200:
                logger.warning(
                    f"[SheetSync] GET {self.webhook_url} returned {r.status_code}"
                )
                return pd.DataFrame()

            data = r.json()
            if isinstance(data, dict) and "data" in data:
                data = data["data"]

            df = pd.DataFrame(data, dtype=str).fillna("")
            logger.info(f"[SheetSync] Sheet currently has {len(df)} existing rows")
            return df

        except Exception as e:
            logger.error(f"[SheetSync] GET to webhook_url failed: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Upsert by EMAIL (preserve workflow-owned columns)
    # ------------------------------------------------------------------
    def _compute_changes(
        self, df_local: pd.DataFrame, df_sheet: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        if df_sheet.empty:
            logger.info("[SheetSync] Sheet empty — all rows are new inserts")
            return df_local.to_dict(orient="records")

        sheet = df_sheet.copy()
        local = df_local.copy()

        sheet["email_norm"] = (
            sheet.get("EMAIL", "").astype(str).str.strip().str.lower()
        )
        local["email_norm"] = (
            local.get("EMAIL", "").astype(str).str.strip().str.lower()
        )

        existing_by_email: Dict[str, Dict[str, Any]] = {}
        for _, r in sheet.iterrows():
            email = r.get("email_norm", "")
            if email:
                existing_by_email[email] = r.to_dict()

        changes: List[Dict[str, Any]] = []
        inserts, updates = 0, 0

        for _, r in local.iterrows():
            row = r.to_dict()
            email_norm = row.get("email_norm", "")
            if not email_norm:
                changes.append({k: row.get(k, "") for k in SHEET_COLUMNS})
                inserts += 1
                continue

            if email_norm not in existing_by_email:
                changes.append({k: row.get(k, "") for k in SHEET_COLUMNS})
                inserts += 1
            else:
                existing = existing_by_email[email_norm]
                merged: Dict[str, Any] = {}
                for col in SHEET_COLUMNS:
                    if col in WORKFLOW_OWNED:
                        merged[col] = existing.get(col, "")
                    elif col in BACKEND_OWNED:
                        merged[col] = row.get(col, "")
                    else:
                        merged[col] = row.get(col, existing.get(col, ""))
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
                    self.webhook_url,
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
                        body = {}

                    if isinstance(body, dict) and body.get("status") == "error":
                        logger.warning(
                            f"[SheetSync] Apps Script logical error: "
                            f"{body.get('message')}"
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
    def sync(self, csv_path: str | None = None) -> int:
        logger.info("=" * 80)
        logger.info("🚀 [SheetSync] Starting sheet sync v2")
        logger.info("=" * 80)

        df_local = self._load_local_rows(
            Path(csv_path) if csv_path is not None else None
        )
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

