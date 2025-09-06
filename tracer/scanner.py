# tracer/scanner.py
import re
import logging
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict

from tracer.tracker import append_point

log = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Patterns to extract player name + coordinates from DayZ ADM lines
# NOTE: DayZ prints position triples as <x, z, y>  (altitude last).
# --------------------------------------------------------------------

# Time prefix seen in Nitrado ADM: "HH:MM:SS | "
RE_TIME_PREFIX = re.compile(r"^\s*(?P<hh>\d{2}):(?P<mm>\d{2}):(?P<ss>\d{2})\s+\|\s*")

# Common "pos=<x,z,y>" lines:
#   15:44:16 | Player "SoulTatted94" (...) pos=<5188.7, 10319.5, 191.2> ...
RE_POS = re.compile(
    r'Player\s+"(?P<name>[^"]+)"[^<]*?pos=<\s*'
    r'(?P<x>-?\d+(?:\.\d+)?)\s*,\s*'
    r'(?P<z>-?\d+(?:\.\d+)?)\s*,\s*'
    r'(?P<y>-?\d+(?:\.\d+)?)\s*>',
    re.IGNORECASE,
)

# Teleport lines — record the *destination* coords (the "... to: <x,z,y>" triple):
#   ... Player "Foo" ... was teleported from: <...> to: <x,z,y>
RE_TP = re.compile(
    r'Player\s+"(?P<name>[^"]+)"[^\n]*?\bteleport(?:ed)?\b[^\n]*?\bto:\s*<\s*'
    r'(?P<x>-?\d+(?:\.\d+)?)\s*,\s*'
    r'(?P<z>-?\d+(?:\.\d+)?)\s*,\s*'
    r'(?P<y>-?\d+(?:\.\d+)?)\s*>',
    re.IGNORECASE,
)

# Fallback: sometimes action lines include a bare "<x,z,y>" after the name.
# Only used for lines that clearly describe an action to reduce false positives.
RE_FALLBACK = re.compile(
    r'Player\s+"(?P<name>[^"]+)"[^\n]*?<\s*'
    r'(?P<x>-?\d+(?:\.\d+)?)\s*,\s*'
    r'(?P<z>-?\d+(?:\.\d+)?)\s*,\s*'
    r'(?P<y>-?\d+(?:\.\d+)?)\s*>',
    re.IGNORECASE,
)

# Suppress trivial wiggles (in X/Z). 0 means "only drop exact duplicates".
MIN_DXZ = 0.0

def _dxz(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    dx = a[0] - b[0]
    dz = a[1] - b[1]
    return (dx * dx + dz * dz) ** 0.5

# Remember last X/Z emitted per player to de-dupe adjacent points.
_last_xz: Dict[str, Tuple[float, float]] = {}

def _maybe_parse_ts_prefix(line: str, fallback: datetime) -> datetime:
    """
    If the ADM line begins with 'HH:MM:SS |', build a UTC datetime using today's date.
    Otherwise return the provided fallback timestamp.
    """
    m = RE_TIME_PREFIX.match(line)
    if not m:
        return fallback
    try:
        hh = int(m.group("hh"))
        mm = int(m.group("mm"))
        ss = int(m.group("ss"))
        base = fallback.astimezone(timezone.utc)
        return datetime(base.year, base.month, base.day, hh, mm, ss, tzinfo=timezone.utc)
    except Exception:
        return fallback

def _emit_point(
    name: str,
    x: float,
    z: float,
    y: float,
    ts: datetime,
    source: str,
    guild_id: Optional[int],
) -> bool:
    """Append to the tracker if not a trivial duplicate; True if appended."""
    xz = (float(x), float(z))
    last = _last_xz.get(name)
    if last is not None and _dxz(last, xz) <= MIN_DXZ:
        return False

    # tracker.append_point expects (x, y, z) with y = altitude; z = north/south.
    append_point(name, float(x), float(y), float(z), ts=ts, source=source, guild_id=guild_id)
    _last_xz[name] = xz
    log.debug(f"scanner: +point [{name}] @ ({x},{z}) via {source}")
    return True


async def scan_adm_line(guild_id: int, line: str, source_ref: str, timestamp: datetime):
    """
    Entry point used by the poller (signature matches LineCallback).
    Extracts {name,x,z,y} from ADM lines and forwards to tracker.
    """
    # Prefer the HH:MM:SS in the line when available so points line up to ADM time.
    event_ts = _maybe_parse_ts_prefix(line, timestamp)

    m = RE_POS.search(line)
    if not m:
        m = RE_TP.search(line)

    if not m and (
        "performed" in line
        or "placed" in line
        or "teleport" in line
        or "was teleported" in line
        or "connected" in line
    ):
        m = RE_FALLBACK.search(line)

    if not m:
        return  # Not a positional line we care about.

    name = m.group("name").strip()
    try:
        x = float(m.group("x"))
        z = float(m.group("z"))  # second value is Z in DayZ logs
        y = float(m.group("y"))  # altitude (third)
    except Exception:
        return  # Parse failure; ignore.

    if _emit_point(name, x, z, y, ts=event_ts, source=source_ref, guild_id=guild_id):
        # Light INFO so you can confirm players are being captured.
        log.info(f"Tracked [{name}] at ({x:.1f},{z:.1f}) from {source_ref}")


# Backwards-compatible alias (if other code imports a different name)
ingest_line = scan_adm_line
