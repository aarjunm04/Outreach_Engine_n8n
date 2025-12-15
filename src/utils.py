import yaml
from pathlib import Path
from loguru import logger
import pandas as pd

# ------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------
LOG_PATH = Path("data/logs.txt")
logger.add(LOG_PATH, rotation="1 MB", retention=5, level="INFO")


# ------------------------------------------------------
# YAML Loader
# ------------------------------------------------------
def load_yaml(path: str | Path, default=None):
    """
    Safely load YAML config files.
    If missing, return default.
    """
    path = Path(path)
    if not path.exists():
        logger.warning(f"[CONFIG] Missing YAML: {path}, using default.")
        return default

    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"[CONFIG] Failed reading YAML {path}: {e}")
        return default


# ------------------------------------------------------
# DataFrame Helpers
# ------------------------------------------------------
def save_df(df: pd.DataFrame, path: str | Path):
    """
    Save DataFrame to CSV with logging + safety.
    """
    try:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        logger.info(f"[DATA] Saved → {path} ({len(df)} rows)")
    except Exception as e:
        logger.error(f"[DATA] Save failed for {path}: {e}")


def read_df(path: str | Path):
    """
    Load CSV safely. Returns empty DF if missing.
    """
    path = Path(path)
    if not path.exists():
        logger.warning(f"[DATA] Missing file: {path}. Returning empty DataFrame.")
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except Exception as e:
        logger.error(f"[DATA] Failed loading {path}: {e}")
        return pd.DataFrame()