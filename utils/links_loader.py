# utils/links_loader.py
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Tuple, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from utils.storageClient import load_file  # already in your project
from utils.settings import get_guild_setting  # assumes you store per-guild flags here

logger = logging.getLogger(__name__)

# Local fallback paths (adjust if your repo differs)
LOCAL_LINKED_PLAYERS_PATHS = [
    "settings/linked_players.json",  # seen in your repo tree
    "data/linked_players.json",      # external service uses /data/...; keep as alt
]

# Simple cache to avoid hammering the external endpoint
_CACHE_TTL_SEC = 60  # keep fresh enough; bump up/down as you wish
_cache_by_guild: dict[int, Tuple[float, dict]] = {}  # guild_id -> (ts, data)


def _read_http_json(url: str, timeout: float = 10.0) -> dict:
    """
    Fetch JSON via HTTP(S). Returns dict (or raises).
    - Sets a UA.
    - Accepts gzip/deflate if server provides it (urllib handles transparently).
    """
    req = Request(url, headers={"User-Agent": "SV-Bounties/links-loader"})
    with urlopen(req, timeout=timeout) as resp:  # nosec - trusted admin-provided URL
        charset = resp.headers.get_content_charset() or "utf-8"
        raw = resp.read()
    try:
        return json.loads(raw.decode(charset, errors="replace"))
    except Exception as e:
        raise ValueError(f"Invalid JSON from {url}: {e}") from e


def _read_local_json() -> tuple[Optional[str], Optional[dict]]:
    """
    Try the known local paths in order and return (path_used, data) if found/valid.
    """
    for p in LOCAL_LINKED_PLAYERS_PATHS:
        try:
            # Use your storageClient for consistency if it resolves relative paths;
            # else fall back to plain file read.
            data = load_file(p)
            if data is None:
                # storageClient didn’t find it; try filesystem
                path = Path(p)
                if path.exists() and path.is_file():
                    data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return p, data
        except Exception:
            logger.debug(f"Local linked_players not usable at {p}", exc_info=True)
    return None, None


def _validate_links(doc: Any) -> dict:
    """
    Schema-light validation so we don’t reject older formats.
    We only require a dict top-level. We also compute a cheap size hint.
    """
    if not isinstance(doc, dict):
        raise ValueError("linked_players.json must be a JSON object at the top level.")
    return doc


def _should_use_external_first(guild_id: int) -> bool:
    # flags you already configure with your slash commands:
    prefer_external = bool(get_guild_setting(guild_id, "prefer_external_links", True))
    disable_local   = bool(get_guild_setting(guild_id, "disable_local_link", False))
    # If local is disabled, we “prefer” external by definition.
    return prefer_external or disable_local


def _external_url(guild_id: int) -> Optional[str]:
    # you set this with /setexternallinks
    url = get_guild_setting(guild_id, "external_links_source", "") or ""
    url = url.strip()
    return url or None


def _count_links_hint(doc: dict) -> int:
    """
    Best-effort size hint for logging. We look for common keys but don’t
    require them.
    """
    # Try a few likely layouts
    for k in ("links", "players", "mapping", "map", "by_id", "by_name"):
        v = doc.get(k)
        if isinstance(v, dict):
            return len(v)
        if isinstance(v, list):
            return len(v)
    # fallback: count top-level keys
    return len(doc)


def get_linked_players(guild_id: int, *, force_refresh: bool = False) -> tuple[dict, str]:
    """
    Load linked_players for this guild.

    Returns:
        (data, source_str)
        - data: dict (validated)
        - source_str: short description of the source used
    """
    now = time.time()
    cached = _cache_by_guild.get(guild_id)
    if cached and not force_refresh:
        ts, data = cached
        if now - ts <= _CACHE_TTL_SEC:
            return data, "cache"

    use_external_first = _should_use_external_first(guild_id)
    url = _external_url(guild_id)

    tried_sources: list[str] = []

    # 1) External (if desired and configured)
    if use_external_first and url:
        tried_sources.append(f"external:{url}")
        try:
            doc = _validate_links(_read_http_json(url))
            _cache_by_guild[guild_id] = (now, doc)
            logger.info(
                f"[Guild {guild_id}] linked_players loaded from EXTERNAL ({_count_links_hint(doc)} entries)."
            )
            return doc, f"external:{url}"
        except (HTTPError, URLError, TimeoutError, ValueError) as e:
            logger.warning(f"[Guild {guild_id}] external links unavailable: {e}")

    # 2) Local fallback (unless explicitly disabled)
    disable_local = bool(get_guild_setting(guild_id, "disable_local_link", False))
    if not disable_local:
        path, doc = _read_local_json()
        tried_sources.append(f"local:{path or 'none'}")
        if isinstance(doc, dict):
            doc = _validate_links(doc)
            _cache_by_guild[guild_id] = (now, doc)
            logger.info(
                f"[Guild {guild_id}] linked_players loaded from LOCAL:{path} ({_count_links_hint(doc)} entries)."
            )
            return doc, f"local:{path}"

    # 3) Try external again if we didn’t try already (e.g., external-first was False)
    if not use_external_first and url:
        tried_sources.append(f"external:{url}")
        try:
            doc = _validate_links(_read_http_json(url))
            _cache_by_guild[guild_id] = (now, doc)
            logger.info(
                f"[Guild {guild_id}] linked_players loaded from EXTERNAL ({_count_links_hint(doc)} entries)."
            )
            return doc, f"external:{url}"
        except (HTTPError, URLError, TimeoutError, ValueError) as e:
            logger.warning(f"[Guild {guild_id}] external links unavailable (second attempt): {e}")

    # Total failure
    raise RuntimeError(
        f"[Guild {guild_id}] Could not load linked_players.json (tried: {', '.join(tried_sources)})"
    )
