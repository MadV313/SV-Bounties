# tracer/tracker.py
import os, time, asyncio
import logging
from datetime import datetime, timezone
from utils.storageClient import load_file, save_file  # your existing helpers
from tracer.config import INDEX_PATH, TRACKS_DIR, MAX_POINTS_PER_PLAYER

logger = logging.getLogger(__name__)

# --- NEW: simple subscription bus for "point appended" events ---
_point_subscribers: list = []  # list[Callable[[int|None,str,dict], Awaitable[None]]]

def subscribe_to_points(callback):
    """callback(guild_id:int|None, gamertag:str, point:dict) -> Awaitable[None]"""
    _point_subscribers.append(callback)
    logger.debug(f"Registered point subscriber: {getattr(callback, '__name__', str(callback))}")

async def _notify_point(guild_id, gamertag, point):
    for cb in list(_point_subscribers):
        try:
            coro = cb(guild_id, gamertag, point)
            if asyncio.iscoroutine(coro):
                await coro
        except Exception as e:
            logger.error(f"Point subscriber error for [{gamertag}]: {e}", exc_info=True)
# ----------------------------------------------------------------

def _sanitize_id(name:str)->str: return name.lower()

def _resolve_player_id(gamertag:str):
    index = load_file(INDEX_PATH) or {}
    pid = index.get(gamertag) or index.get(gamertag.lower())
    if not pid:
        pid = f"xbox-{_sanitize_id(gamertag)}"
        index[gamertag] = pid
        index[gamertag.lower()] = pid
        save_file(INDEX_PATH, index)
        logger.info(f"Indexed new player: {gamertag} -> {pid}")
    return pid, gamertag

def _track_path(pid:str)->str:
    os.makedirs(TRACKS_DIR, exist_ok=True)
    return f"{TRACKS_DIR}/{pid}.json"

def append_point(gamertag:str, x:float, y:float, z:float,
                 ts:datetime|None=None, source:str="", guild_id:int|None=None):
    """Append a point to the player's track and notify subscribers."""
    pid, canonical = _resolve_player_id(gamertag)
    path = _track_path(pid)
    doc = load_file(path) or {"player_id": pid, "gamertag": canonical, "points": []}
    if not ts:
        ts = datetime.now(timezone.utc)
    doc["gamertag"] = canonical

    # de-dupe adjacent identical X/Z
    if doc["points"] and (doc["points"][-1]["x"], doc["points"][-1]["z"]) == (x, z):
        logger.debug(f"[{canonical}] Duplicate adjacent point ignored at ({x},{z}) from {source}")
        return

    point = {
        "ts": ts.isoformat().replace("+00:00","Z"),
        "x": x, "y": y, "z": z, "source": source
    }
    doc["points"].append(point)
    if len(doc["points"]) > MAX_POINTS_PER_PLAYER:
        doc["points"] = doc["points"][-MAX_POINTS_PER_PLAYER:]
        logger.debug(f"[{canonical}] Track truncated to last {MAX_POINTS_PER_PLAYER} points")

    try:
        save_file(path, doc)
        logger.info(f"Track append [{canonical}] ({x},{z}) total={len(doc['points'])}")
    except Exception as e:
        logger.error(f"Failed to save track for {canonical} at {path}: {e}", exc_info=True)
        return

    # NEW: notify listeners (live pulse etc.)
    try:
        asyncio.get_running_loop().create_task(_notify_point(guild_id, canonical, point))
        logger.debug(f"Notified subscribers for [{canonical}] @ ({x},{z})")
    except RuntimeError:
        # no loop (rare); best-effort call without scheduling
        logger.warning("No running event loop; notifying subscribers synchronously.")
        try:
            asyncio.run(_notify_point(guild_id, canonical, point))
        except Exception as e:
            logger.error(f"Synchronous notify failed for {canonical}: {e}", exc_info=True)

def load_track(player_query:str, window_hours:int|None=None, max_points:int|None=None):
    from datetime import datetime
    import time as _time
    index = load_file(INDEX_PATH) or {}
    pid = index.get(player_query) or index.get(player_query.lower())
    if not pid:
        for k, v in index.items():
            if k.lower().startswith(player_query.lower()):
                pid = v; break
    if not pid:
        logger.debug(f"load_track: no index match for query '{player_query}'")
        return None, None
    doc = load_file(_track_path(pid)) or {"player_id": pid, "gamertag": player_query, "points": []}
    pts = doc["points"]
    if window_hours:
        cutoff = _time.time() - window_hours*3600
        before = len(pts)
        pts = [p for p in pts if datetime.fromisoformat(p["ts"].replace("Z","+00:00")).timestamp() >= cutoff]
        logger.debug(f"load_track: window={window_hours}h reduced {before}->{len(pts)} for {doc.get('gamertag')}")
    if max_points and len(pts) > max_points:
        pts = pts[-max_points:]
        logger.debug(f"load_track: limited to last {max_points} points for {doc.get('gamertag')}")
    logger.info(f"Loaded track for {doc.get('gamertag')} with {len(pts)} point(s)")
    return pid, {**doc, "points": pts}
