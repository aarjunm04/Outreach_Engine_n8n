from pathlib import Path
import yaml
from src.sheet_sync import SheetSync

cfg = yaml.safe_load(open(Path('config/settings.yaml')))
webhook = cfg['google_sheets']['webhook_url']
csv_path = "data/enriched_with_emails.csv"  
syncer = SheetSync(webhook)
result = syncer.sync(csv_path)
print('Rows sent:', result)