# tracer/adm_state.py
import json
from pathlib import Path
from typing import Optional

STATE_PATH = "data/adm_state.json"

def _load() -> dict:
    p = Path(STATE_PATH)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save(obj: dict):
    p = Path(STATE_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def get_guild_state(guild_id: int) -> dict:
    data = _load()
    return data.get(str(guild_id), {})

def set_guild_state(guild_id: int, *, latest_file: Optional[str] = None, offset: Optional[int] = None):
    data = _load()
    g = data.get(str(guild_id), {})
    if latest_file is not None:
        g["latest_file"] = latest_file
    if offset is not None:
        g["offset"] = offset
    data[str(guild_id)] = g
    _save(data)
