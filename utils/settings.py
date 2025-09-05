# utils/settings.py
import json
from pathlib import Path
from typing import Dict, Any, Optional

# Legacy single-file (for migration)
LEGACY_SETTINGS_PATH = Path("data/settings.json")
# New per-guild directory
SETTINGS_DIR = Path("data/settings")
SETTINGS_DIR.mkdir(parents=True, exist_ok=True)

# Default settings for a guild
DEFAULT_SETTINGS: Dict[str, Any] = {
    "bounty_channel_id": None,   # public bounties channel
    "admin_channel_id": None,    # private admin channel for trace/logs
    "active_map": "livonia",
    # Optional external links file (SV13 style), set via /setexternallinks
    "external_links_path": None,
}

def _path_for_guild(guild_id: int) -> Path:
    return SETTINGS_DIR / f"{guild_id}.json"

def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def _migrate_legacy_if_present(guild_id: int) -> Optional[Dict[str, Any]]:
    """
    If a legacy data/settings.json exists and no per-guild file does,
    copy what we can as a starting point to the guild file.
    """
    dst = _path_for_guild(guild_id)
    if dst.exists():
        return None
    legacy = _read_json(LEGACY_SETTINGS_PATH)
    if not legacy:
        return None
    # Keep only keys we know about
    migrated = DEFAULT_SETTINGS.copy()
    for k in migrated.keys():
        if k in legacy:
            migrated[k] = legacy[k]
    _write_json(dst, migrated)
    return migrated

def load_settings(guild_id: int) -> Dict[str, Any]:
    """
    Load settings for a guild. Creates a file with defaults if missing.
    Also migrates from legacy settings.json once (best effort).
    """
    p = _path_for_guild(guild_id)
    data = _read_json(p)
    if data is None:
        # attempt migration, else defaults
        data = _migrate_legacy_if_present(guild_id) or DEFAULT_SETTINGS.copy()
        _write_json(p, data)

    # backfill new keys if we add them later
    changed = False
    for k, v in DEFAULT_SETTINGS.items():
        if k not in data:
            data[k] = v
            changed = True
    if changed:
        _write_json(p, data)
    return data

def save_settings(guild_id: int, updates: Dict[str, Any]) -> Dict[str, Any]:
    data = load_settings(guild_id)
    data.update(updates)
    _write_json(_path_for_guild(guild_id), data)
    return data
