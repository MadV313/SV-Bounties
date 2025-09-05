# utils/linking.py
import json
import logging
from pathlib import Path
from typing import Optional, Tuple

from utils.settings import load_settings
from tracer.config import LOCAL_LINKS_PATH

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
    """Per-guild linked players file."""
    return Path(f"data/linked_players/{guild_id}.json")

def load_external_links(guild_id: int) -> dict[str, dict] | None:
    s = load_settings(guild_id)
    src = s.get("external_links_path")
    if not src:
        return None
    if src.startswith("http://") or src.startswith("https://"):
        data = _read_json_url(src)
    else:
        data = _read_json(src)
    if isinstance(data, dict):
        return data
    return None

def load_local_links(guild_id: int) -> dict[str, dict]:
    """Load per-guild local links file. Fallback to {} if missing."""
    p = _local_path_for_guild(guild_id)
    data = _read_json(str(p))
    if isinstance(data, dict):
        return data
    return {}

def save_local_links(guild_id: int, obj: dict):
    p = _local_path_for_guild(guild_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        p.write_text(json.dumps(obj, indent=2), encoding="utf-8")
        logger.debug(f"Saved {len(obj)} local links for guild {guild_id} -> {p}")
    except Exception as e:
        logger.error(f"Failed to save local links for guild {guild_id}: {e}", exc_info=True)

def resolve_from_any(
    guild_id: int,
    discord_id: Optional[str] = None,
    gamertag: Optional[str] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (discord_id, gamertag) if found by either key, searching local first then external.
    """
    local = load_local_links(guild_id)
    ext = load_external_links(guild_id) or {}

    # by discord id
    if discord_id:
        rec = local.get(discord_id) or ext.get(discord_id)
        if rec and isinstance(rec, dict):
            return discord_id, rec.get("gamertag")

    # by gamertag (case-insensitive)
    if gamertag:
        g_lower = gamertag.lower()
        for source in (local, ext):
            for did, rec in source.items():
                if isinstance(rec, dict) and str(rec.get("gamertag", "")).lower() == g_lower:
                    return did, rec.get("gamertag")

    return None, None

def link_locally(guild_id: int, discord_id: str, gamertag: str, platform: str = "xbox"):
    """Store link only in this guild's local links file."""
    links = load_local_links(guild_id)
    links[discord_id] = {"gamertag": gamertag, "platform": platform}
    save_local_links(guild_id, links)
