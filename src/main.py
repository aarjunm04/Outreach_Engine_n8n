# ============================================================================
# src/main.py - Outreach Engine Orchestration
# ============================================================================
"""
Main orchestration script:
1. Scraper (SerpAPI + proxies) → scraper_output.csv
2. Dedup (against Google Sheet) → deduplicated.csv
3. Enrichment (Hunter.io) → enriched_with_emails.csv
4. Sheets Sync (Apps Script webhook) → Google Sheet

Run with: python -m src.main
"""

import sys
from pathlib import Path

import yaml
from loguru import logger

# Import modules
from .scraper import run_scraper
from .enrichment import run_enrichment
from .sheet_sync import SheetSync


# ============================================================================
# Logging Configuration
# ============================================================================


def setup_logging(config: dict) -> None:
    """Configure loguru logging."""
    log_level = config.get("monthly_run", {}).get("log_level", "INFO")

    logger.remove()
    logger.add(
        sys.stderr,
        format=(
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan> - "
            "<level>{message}</level>"
        ),
        level=log_level,
    )


# ============================================================================
# Main Pipeline
# ============================================================================


def run_full_pipeline() -> bool:
    """
    Execute complete outreach engine pipeline:
    Scraping → Enrichment → Sheets Sync
    """
    # Load config
    config_path = Path(__file__).parent.parent / "config" / "settings.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f) or {}

    # Setup logging
    setup_logging(config)

    logger.info("")
    logger.info("╔" + "=" * 78 + "╗")
    logger.info(
        "║"
        + " " * 20
        + "🚀 OUTREACH ENGINE - PRODUCTION PIPELINE 🚀"
        + " " * 15
        + "║"
    )
    logger.info("╚" + "=" * 78 + "╝")
    logger.info("")

    # ========================================================================
    # STAGE 1: SCRAPING
    # ========================================================================
    logger.info("STAGE 1: SCRAPING")
    logger.info("-" * 80)

    try:
        df_scraped = run_scraper()

        if df_scraped.empty:
            logger.error("❌ Scraping failed. Aborting pipeline.")
            return False

        scraped_count = len(df_scraped)
        logger.info(f"✅ Scraping complete: {scraped_count} profiles")

    except Exception as e:
        logger.error(f"❌ Scraping error: {e}")
        return False

    logger.info("")

    # ========================================================================
    # STAGE 2: ENRICHMENT
    # ========================================================================
    logger.info("STAGE 2: ENRICHMENT")
    logger.info("-" * 80)

    try:
        df_enriched = run_enrichment()

        if df_enriched.empty:
            logger.error("❌ Enrichment returned no profiles")
            enriched_count = 0
        else:
            enriched_count = len(df_enriched)
            logger.info(
                f"✅ Enrichment complete: {enriched_count} profiles with emails"
            )

    except Exception as e:
        logger.error(f"❌ Enrichment error: {e}")
        return False

    logger.info("")

    # ========================================================================
    # STAGE 3: GOOGLE SHEETS SYNC
    # ========================================================================
    logger.info("STAGE 3: GOOGLE SHEETS SYNC")
    logger.info("-" * 80)

    # Get sheet sync config from settings
    webhook_url = config.get("google_sheets", {}).get("webhook_url")

    if not webhook_url:
        logger.error("❌ Missing 'webhook_url' in config/settings.yaml")
        return False

    try:
        # Initialize and run sheet sync (reads data/enriched_with_emails.csv)
        sheet_sync = SheetSync(webhook_url=webhook_url)
        rows_sent = sheet_sync.sync()

        if rows_sent == 0:
            logger.warning("⚠️ Sheet sync completed but no rows were sent")
        else:
            logger.info(f"✅ Sheet sync complete: {rows_sent} rows synced")

    except Exception as e:
        logger.error(f"❌ Sheet sync error: {e}")
        return False

    logger.info("")

    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    logger.info("")
    logger.info("╔" + "=" * 78 + "╗")
    logger.info("║" + " " * 25 + "✅ PIPELINE COMPLETE ✅" + " " * 29 + "║")
    logger.info("║" + " " * 78 + "║")
    logger.info(f"║ Scraped:  {scraped_count:>6} profiles" + " " * 57 + "║")
    logger.info(
        f"║ Enriched: {enriched_count:>6} profiles with verified emails"
        + " " * 35
        + "║"
    )
    logger.info("║" + " " * 78 + "║")
    logger.info(
        "║ Next: n8n workflow will send cold emails from Google Sheet"
        + " " * 16
        + "║"
    )
    logger.info("╚" + "=" * 78 + "╝")
    logger.info("")

    return True


def main() -> None:
    """Main entry point."""
    try:
        success = run_full_pipeline()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

