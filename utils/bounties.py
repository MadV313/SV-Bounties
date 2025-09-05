# utils/bounties.py
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

BOUNTY_PATH = "data/bounties.json"

def _load() -> dict:
    p = Path(BOUNTY_PATH)
    if not p.exists():
        return {"open": [], "closed": []}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"open": [], "closed": []}

def _save(obj: dict):
    p = Path(BOUNTY_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def create_bounty(creator_id: str, target_discord_id: str | None, target_gamertag: str,
                  amount: int, note: str | None = None):
    data = _load()
    bounty = {
        "id": f"bnty-{int(datetime.now(timezone.utc).timestamp())}",
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "creator_id": creator_id,
        "target_discord_id": target_discord_id,
        "target_gamertag": target_gamertag,
        "amount": amount,
        "note": note,
        "status": "open",
        "kills": []
    }
    data["open"].append(bounty)
    _save(data)
    return bounty

def close_bounty(bounty_id: str, killer_discord_id: str | None, killer_gamertag: str | None):
    data = _load()
    bounty = next((b for b in data["open"] if b["id"] == bounty_id), None)
    if not bounty:
        return None
    bounty["status"] = "closed"
    bounty["closed_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    bounty["kills"].append({
        "killer_discord_id": killer_discord_id,
        "killer_gamertag": killer_gamertag
    })
    data["open"] = [b for b in data["open"] if b["id"] != bounty_id]
    data["closed"].append(bounty)
    _save(data)
    return bounty

def list_open():
    return _load()["open"]

# --- New helpers for removing bounties ---
def remove_bounty_by_gamertag(gamertag: str) -> int:
    """Remove all open bounties for a given gamertag (case-insensitive). Returns number removed."""
    data = _load()
    g = gamertag.lower()
    before = len(data["open"])
    data["open"] = [b for b in data["open"] if str(b.get("target_gamertag", "")).lower() != g]
    removed = before - len(data["open"])
    _save(data)
    return removed

def remove_bounty_by_discord_id(discord_id: str) -> int:
    """Remove all open bounties for a given Discord ID. Returns number removed."""
    data = _load()
    before = len(data["open"])
    data["open"] = [b for b in data["open"] if str(b.get("target_discord_id") or "") != str(discord_id)]
    removed = before - len(data["open"])
    _save(data)
    return removed

def clear_all_bounties() -> int:
    """Clear ALL open bounties. Returns number cleared."""
    data = _load()
    n = len(data["open"])
    data["open"] = []
    _save(data)
    return n
