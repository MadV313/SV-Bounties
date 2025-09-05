# tracer/tracker.py
import os, time, asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict
from collections import defaultdict, deque

from utils.storageClient import load_file, save_file  # your existing helpers
from tracer.config import INDEX_PATH, TRACKS_DIR, MAX_POINTS_PER_PLAYER

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Logging throttling (reduce spam without losing data)
# -----------------------------------------------------------------------------
# How often (in seconds) we allow an INFO "append" log per player
THROTTLE_APPEND_SECS = 5.0
# How often we allow an INFO "indexed new player" log for the same tag
THROTTLE_INDEX_SECS = 30.0

_last_log_ts: Dict[str, float] = {}

def _should_log(key: str, interval: float) -> bool:
    """Return True if enough time has passed since last log for this key."""
    now = time.monotonic()
    last = _last_log_ts.get(key, 0.0)
    if now - last >= interval:
        _last_log_ts[key] = now
        return True
    return False
# -----------------------------------------------------------------------------


# Normalize TRACKS_DIR as a Path
_TRACKS_DIR_PATH = Path(TRACKS_DIR)

# --- Ensure track directory is valid -----------------------------------------
def _ensure_tracks_dir() -> None:
    """
    Make sure TRACKS_DIR exists and is a directory.
    If a file exists at that path (common repo mistake), rename it to .bak and create the dir.
    """
    try:
        if _TRACKS_DIR_PATH.exists():
            if _TRACKS_DIR_PATH.is_file():
                backup = _TRACKS_DIR_PATH.with_suffix(_TRACKS_DIR_PATH.suffix + ".bak")
                try:
                    _TRACKS_DIR_PATH.rename(backup)
                    logger.warning(
                        f"TRACKS_DIR path existed as a file; moved it to {backup} and will create a directory."
                    )
                except Exception as e:
                    logger.error(f"Failed to move file { _TRACKS_DIR_PATH } -> { backup }: {e}", exc_info=True)
                    raise
        # Create directory if missing
        _TRACKS_DIR_PATH.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"Unable to ensure track directory at { _TRACKS_DIR_PATH }: {e}", exc_info=True)
        raise

# Call once at import so we fail fast (and self-heal if needed)
_ensure_tracks_dir()
# -----------------------------------------------------------------------------


# --- Simple subscription bus for "point appended" events ---------------------
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
# -----------------------------------------------------------------------------


def _sanitize_id(name: str) -> str:
    return name.lower()


def _resolve_player_id(gamertag: str):
    index = load_file(INDEX_PATH) or {}
    pid = index.get(gamertag) or index.get(gamertag.lower())
    if not pid:
        pid = f"xbox-{_sanitize_id(gamertag)}"
        index[gamertag] = pid
        index[gamertag.lower()] = pid
        save_file(INDEX_PATH, index)
        # Throttle this INFO so repeated reconnects don’t spam
        if _should_log(f"index:{pid}", THROTTLE_INDEX_SECS):
            logger.info(f"Indexed new player: {gamertag} -> {pid}")
        else:
            logger.debug(f"Indexed new player (throttled): {gamertag} -> {pid}")
    return pid, gamertag


def _track_path(pid: str) -> str:
    # Defensive: ensure again before each write/read in case runtime state changed
    _ensure_tracks_dir()
    return str(_TRACKS_DIR_PATH / f"{pid}.json")


# =============================================================================
# Buffered writes to avoid rate limits / excessive FS churn
# =============================================================================
# Per-player in-memory queues of new points (dicts)
_buffers: Dict[str, deque] = defaultdict(deque)
# Last time we flushed any buffer (epoch seconds)
_last_flush_ts: float = 0.0
# Flush policy
_FLUSH_INTERVAL = 15        # seconds: flush all buffers at least this often
_MAX_BUFFER_POINTS = 10     # flush a player's buffer if it reaches this size


def _flush_pid(pid: str, doc_gamertag_fallback: str | None = None) -> None:
    """Flush the buffer for a single player ID, if any."""
    q = _buffers.get(pid)
    if not q:
        return

    path = _track_path(pid)
    doc = load_file(path) or {"player_id": pid,
                              "gamertag": doc_gamertag_fallback or "unknown",
                              "points": []}
    # extend with queued points, but drop the helper field "gamertag" on write
    new_pts = list(q)
    for p in new_pts:
        p.pop("gamertag", None)
    doc["points"].extend(new_pts)

    if len(doc["points"]) > MAX_POINTS_PER_PLAYER:
        doc["points"] = doc["points"][-MAX_POINTS_PER_PLAYER:]

    try:
        save_file(path, doc)
        logger.debug(f"Flushed {len(new_pts)} pts for {doc.get('gamertag')} (total={len(doc['points'])})")
    except Exception as e:
        logger.error(f"Failed to flush track for {pid}: {e}", exc_info=True)

    q.clear()


def _flush_maybe(force: bool = False) -> None:
    """Flush all player buffers if interval passed or force=True."""
    global _last_flush_ts
    now = time.time()
    if force or (now - _last_flush_ts) >= _FLUSH_INTERVAL:
        for pid in list(_buffers.keys()):
            _flush_pid(pid)
        _last_flush_ts = now
# =============================================================================


def append_point(
    gamertag: str,
    x: float,
    y: float,
    z: float,
    ts: datetime | None = None,
    source: str = "",
    guild_id: int | None = None,
):
    """Append a point to the player's track and notify subscribers (buffered)."""
    pid, canonical = _resolve_player_id(gamertag)
    if not ts:
        ts = datetime.now(timezone.utc)

    # Build queued point (we keep gamertag only for logging/flush default)
    point = {
        "ts": ts.isoformat().replace("+00:00", "Z"),
        "x": x,
        "y": y,
        "z": z,
        "source": source,
        "gamertag": canonical,
    }

    q = _buffers[pid]

    # De-dupe adjacent identical X/Z (check buffer last if present,
    # else peek last saved point once when buffer is empty)
    if q:
        if (q[-1]["x"], q[-1]["z"]) == (x, z):
            logger.debug(f"[{canonical}] Duplicate adjacent point ignored at ({x},{z}) from {source}")
            return
    else:
        # Buffer empty: peek last saved point to avoid immediate duplicates
        path = _track_path(pid)
        doc = load_file(path)
        if doc and doc.get("points"):
            last = doc["points"][-1]
            if (last.get("x"), last.get("z")) == (x, z):
                logger.debug(f"[{canonical}] Duplicate (vs saved) point ignored at ({x},{z}) from {source}")
                return

    # Queue and maybe flush
    q.append(point)

    # Throttled "append" log (buffered)
    if _should_log(f"append:{pid}", THROTTLE_APPEND_SECS):
        logger.info(f"Track append [{canonical}] ({x},{z}) (buffered size={len(q)})")
    else:
        logger.debug(f"Track append (throttled) [{canonical}] ({x},{z}) buf={len(q)}")

    # Flush rules: per-player size threshold OR global interval
    if len(q) >= _MAX_BUFFER_POINTS:
        _flush_pid(pid, doc_gamertag_fallback=canonical)
    _flush_maybe(force=False)

    # Notify listeners (live pulse etc.)
    try:
        asyncio.get_running_loop().create_task(_notify_point(guild_id, canonical, dict(point)))
        logger.debug(f"Notified subscribers for [{canonical}] @ ({x},{z})")
    except RuntimeError:
        # No running loop: best-effort synchronous call
        logger.warning("No running event loop; notifying subscribers synchronously.")
        try:
            asyncio.run(_notify_point(guild_id, canonical, dict(point)))
        except Exception as e:
            logger.error(f"Synchronous notify failed for {canonical}: {e}", exc_info=True)


def load_track(player_query: str, window_hours: int | None = None, max_points: int | None = None):
    from datetime import datetime as _dt
    import time as _time

    index = load_file(INDEX_PATH) or {}
    pid = index.get(player_query) or index.get(player_query.lower())
    if not pid:
        for k, v in index.items():
            if k.lower().startswith(player_query.lower()):
                pid = v
                break
    if not pid:
        logger.debug(f"load_track: no index match for query '{player_query}'")
        return None, None

    # Ensure any buffered points for this player are flushed before read
    _flush_pid(pid)

    doc = load_file(_track_path(pid)) or {"player_id": pid, "gamertag": player_query, "points": []}
    pts = doc["points"]

    if window_hours:
        cutoff = _time.time() - window_hours * 3600
        before = len(pts)
        pts = [p for p in pts if _dt.fromisoformat(p["ts"].replace("Z", "+00:00")).timestamp() >= cutoff]
        logger.debug(f"load_track: window={window_hours}h reduced {before}->{len(pts)} for {doc.get('gamertag')}")

    if max_points and len(pts) > max_points:
        pts = pts[-max_points:]
        logger.debug(f"load_track: limited to last {max_points} points for {doc.get('gamertag')}")

    logger.info(f"Loaded track for {doc.get('gamertag')} with {len(pts)} point(s)")
    return pid, {**doc, "points": pts}
