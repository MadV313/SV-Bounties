# utils/linking.py
import json
from pathlib import Path
from typing import Optional, Tuple

from utils.settings import load_settings
from tracer.config import LOCAL_LINKS_PATH

def _read_json(path: str) -> dict | list | None:
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def _read_json_url(url: str) -> dict | list | None:
    # Simple URL fetch (supports raw GitHub / http(s)). You can replace with your existing storageClient if needed.
    try:
        import urllib.request
        with urllib.request.urlopen(url, timeout=8) as resp:
            return json.load(resp)
    except Exception:
        return None

def load_external_links() -> dict[str, dict] | None:
    s = load_settings()
    src = s.get("external_links_path")
    if not src:
        return None
    if src.startswith("http://") or src.startswith("https://"):
        data = _read_json_url(src)
    else:
        data = _read_json(src)
    # normalize to {discord_id:{gamertag:..., platform:...}}
    if isinstance(data, dict):
        return data
    return None

def load_local_links() -> dict[str, dict]:
    data = _read_json(LOCAL_LINKS_PATH)
    if isinstance(data, dict):
        return data
    return {}

def save_local_links(obj: dict):
    p = Path(LOCAL_LINKS_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def resolve_from_any(discord_id: Optional[str]=None, gamertag: Optional[str]=None) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (discord_id, gamertag) if found by either key, searching local first then external.
    """
    local = load_local_links()
    ext = load_external_links() or {}
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
                if isinstance(rec, dict) and str(rec.get("gamertag","")).lower() == g_lower:
                    return did, rec.get("gamertag")
    return None, None

def link_locally(discord_id: str, gamertag: str, platform: str="xbox"):
    links = load_local_links()
    links[discord_id] = {"gamertag": gamertag, "platform": platform}
    save_local_links(links)
