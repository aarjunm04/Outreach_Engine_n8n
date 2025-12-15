#!/bin/bash

DOW=$(date +%u)   # 3 = Wednesday
DAY=$(date +%d)   # 01..31

# Only continue on the 2nd Wednesday of the month
if ! { [ "$DOW" -eq 3 ] && [ "$DAY" -ge 8 ] && [ "$DAY" -le 14 ]; }; then
  exit 0
fi

PROJECT_ROOT="/Users/apple/TechStack/Projects/Outreach_Engine"

cd "$PROJECT_ROOT" || exit 1

# Activate venv
source "$PROJECT_ROOT/venv/bin/activate"

# Optional: clean old CSVs
rm -f "$PROJECT_ROOT/data/scraper_output.csv"
rm -f "$PROJECT_ROOT/data/enriched_with_emails.csv"

# Run full pipeline (scraper -> enrichment -> sheet sync)
python -m src.main >> "$PROJECT_ROOT/logs/pipeline_$(date +%F).log" 2>&1

