# utils/settings.py
import json
from pathlib import Path
from tracer.config import SETTINGS_PATH, DEFAULT_SETTINGS

def _read_json(path: str):
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def _write_json(path: str, obj):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(obj, indent=2), encoding="utf-8")

def load_settings():
    data = _read_json(SETTINGS_PATH)
    if not data:
        data = DEFAULT_SETTINGS.copy()
        _write_json(SETTINGS_PATH, data)
    # backfill any new keys
    changed = False
    for k, v in DEFAULT_SETTINGS.items():
        if k not in data:
            data[k] = v
            changed = True
    if changed:
        _write_json(SETTINGS_PATH, data)
    return data

def save_settings(updates: dict):
    data = load_settings()
    data.update(updates)
    _write_json(SETTINGS_PATH, data)
    return data
