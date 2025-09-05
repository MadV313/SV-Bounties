# tracer/adm_state.py
import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

STATE_PATH = "data/adm_state.json"

def _load() -> dict:
    p = Path(STATE_PATH)
    if not p.exists():
        logger.debug("ADM state file not found, returning empty dict.")
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Failed to read ADM state file {STATE_PATH}: {e}", exc_info=True)
        return {}

def _save(obj: dict):
    p = Path(STATE_PATH)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(obj, indent=2), encoding="utf-8")
        logger.debug(f"ADM state saved for {len(obj)} guild(s).")
    except Exception as e:
        logger.error(f"Failed to save ADM state file {STATE_PATH}: {e}", exc_info=True)

def get_guild_state(guild_id: int) -> dict:
    data = _load()
    state = data.get(str(guild_id), {})
    logger.debug(f"Loaded ADM state for guild {guild_id}: {state}")
    return state

def set_guild_state(guild_id: int, *, latest_file: Optional[str] = None, offset: Optional[int] = None):
    data = _load()
    g = data.get(str(guild_id), {})
    if latest_file is not None:
        g["latest_file"] = latest_file
        logger.info(f"[Guild {guild_id}] ADM state updated latest_file={latest_file}")
    if offset is not None:
        g["offset"] = offset
        logger.debug(f"[Guild {guild_id}] ADM state updated offset={offset}")
    data[str(guild_id)] = g
    _save(data)
