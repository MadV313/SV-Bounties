# utils/linking.py
import json
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

from utils.settings import load_settings
from tracer.config import LOCAL_LINKS_PATH  # kept for compatibility; not directly used now

logger = logging.getLogger(__name__)

def _read_json(path: str) -> dict | list | None:
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Failed to read JSON from {path}: {e}", exc_info=True)
        return None

def _read_json_url(url: str) -> dict | list | None:
    try:
        import urllib.request
        with urllib.request.urlopen(url, timeout=8) as resp:
            return json.load(resp)
    except Exception as e:
        logger.error(f"Failed to fetch JSON from {url}: {e}", exc_info=True)
        return None

def _local_path_for_guild(guild_id: int) -> Path:
    """Per-guild linked players file (local)."""
    return Path(f"data/linked_players/{guild_id}.json")

def _normalize_links_map(raw: Any) -> Dict[str, Dict[str, Any]]:
    """
    Normalize various input shapes into: { "<discord_id>": {"gamertag": str, ...} }
    Accepts:
      - {id: "Gamertag"}                   -> {"gamertag": "..."}
      - {id: {"gamertag": "...", ...}}     -> kept as-is (keys preserved)
    Ignores non-dict top-levels by returning {}.
    """
    out: Dict[str, Dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return out
    for did, rec in raw.items():
        if isinstance(rec, str):
            out[str(did)] = {"gamertag": rec}
        elif isinstance(rec, dict):
            # Ensure 'gamertag' key exists if there’s a likely candidate
            if "gamertag" in rec:
                out[str(did)] = {**rec}
            else:
                # Try a couple heuristics (rare)
                gt = rec.get("tag") or rec.get("name") or rec.get("xbox") or rec.get("steam")
                if isinstance(gt, str):
                    out[str(did)] = {"gamertag": gt, **rec}
                else:
                    # Keep as-is; resolve_from_any will just fail to match if no 'gamertag'
                    out[str(did)] = {**rec}
        else:
            # Unexpected type; skip
            continue
    return out

def load_external_links(guild_id: int) -> Dict[str, Dict[str, Any]] | None:
    """
    Load external links map (per guild) and normalize to {id: {gamertag: ...}}.
    Returns None if not configured or failed to load.
    """
    s = load_settings(guild_id)
    src = s.get("external_links_path")
    if not src:
        return None
    if src.startswith("http://") or src.startswith("https://"):
        data = _read_json_url(src)
    else:
        data = _read_json(src)
    if isinstance(data, dict):
        return _normalize_links_map(data)
    return None

def load_local_links(guild_id: int) -> Dict[str, Dict[str, Any]]:
    """
    Load per-guild local links file and normalize to {id: {gamertag: ...}}.
    Fallback to {} if missing.
    """
    p = _local_path_for_guild(guild_id)
    data = _read_json(str(p))
    if isinstance(data, dict):
        return _normalize_links_map(data)
    return {}

def save_local_links(guild_id: int, obj: dict):
    """
    Save per-guild local links file. Accepts either normalized form or the flat {id:"tag"} form.
    We normalize before saving to keep consistency.
    """
    p = _local_path_for_guild(guild_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        normalized = _normalize_links_map(obj)
        p.write_text(json.dumps(normalized, indent=2), encoding="utf-8")
        logger.debug(f"Saved {len(normalized)} local links for guild {guild_id} -> {p}")
    except Exception as e:
        logger.error(f"Failed to save local links for guild {guild_id}: {e}", exc_info=True)

def resolve_from_any(
    guild_id: int,
    discord_id: Optional[str] = None,
    gamertag: Optional[str] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Resolve link by Discord ID or by gamertag.
    Honors per-guild preference:
      - prefer_external_links: when True, external map is checked first.
    Returns (discord_id, gamertag) or (None, None) if not found.
    """
    s = load_settings(guild_id)
    prefer_ext = bool(s.get("prefer_external_links"))

    local = load_local_links(guild_id)
    ext = load_external_links(guild_id) or {}

    sources = (ext, local) if prefer_ext else (local, ext)

    # by discord id
    if discord_id:
        for src in sources:
            rec = src.get(discord_id)
            if isinstance(rec, dict):
                gt = rec.get("gamertag")
                if isinstance(gt, str) and gt:
                    return discord_id, gt

    # by gamertag (case-insensitive)
    if gamertag:
        g_lower = gamertag.lower()
        for src in sources:
            for did, rec in src.items():
                if not isinstance(rec, dict):
                    continue
                gt = rec.get("gamertag")
                if isinstance(gt, str) and gt.lower() == g_lower:
                    return did, gt

    return None, None

def link_locally(guild_id: int, discord_id: str, gamertag: str, platform: str = "xbox"):
    """
    Store/overwrite link only in this guild's local links file.
    The saved shape is normalized: { "<id>": {"gamertag": "...", "platform": "xbox"} }
    """
    links = load_local_links(guild_id)
    links[str(discord_id)] = {"gamertag": gamertag, "platform": platform}
    save_local_links(guild_id, links)
