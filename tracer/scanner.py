# tracer/scanner.py
import re
import logging
from datetime import datetime
from typing import Optional, Tuple, Dict

from tracer.tracker import append_point

log = logging.getLogger(__name__)

# Matches the common DayZ ADM lines containing a position:
#   15:44:16 | Player "SoulTatted94" (id=...) pos=<5188.7, 10319.5, 191.2> ...
RE_POS = re.compile(
    r'Player\s+"(?P<name>[^"]+)"[^<]*?pos=<\s*(?P<x>-?\d+(?:\.\d+)?),\s*(?P<y>-?\d+(?:\.\d+)?),\s*(?P<z>-?\d+(?:\.\d+)?)\s*>',
    re.IGNORECASE,
)

# Matches teleport lines; we record the *destination* coords:
#   ... "Player "Foo" ... was teleported from: <x,y,z> to: <x,y,z>
RE_TP = re.compile(
    r'Player\s+"(?P<name>[^"]+)"[^<]*?teleport[^:]*?:.*?to:\s*<\s*(?P<x>-?\d+(?:\.\d+)?),\s*(?P<y>-?\d+(?:\.\d+)?),\s*(?P<z>-?\d+(?:\.\d+)?)\s*>',
    re.IGNORECASE,
)

# In case some lines don’t say "pos=" but still have an immediate triple after the name.
# We'll only use this if the line contains a known action keyword.
RE_FALLBACK = re.compile(
    r'Player\s+"(?P<name>[^"]+)"[^\n]*?<\s*(?P<x>-?\d+(?:\.\d+)?),\s*(?P{y}-?\d+(?:\.\d+)?),\s*(?P<z>-?\d+(?:\.\d+)?)\s*>',
    re.IGNORECASE,
)

# Distance threshold (XZ) to suppress tiny wiggles. 0 means "only exact duplicates are dropped".
MIN_DXZ = 0.0

def _dxz(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    dx = a[0] - b[0]
    dz = a[1] - b[1]
    return (dx * dx + dz * dz) ** 0.5

# Last X/Z we emitted for each player (by name). Keeps the stream clean.
_last_xz: Dict[str, Tuple[float, float]] = {}

def _emit_point(name: str, x: float, y: float, z: float,
                ts: datetime, source: str, guild_id: Optional[int]) -> bool:
    """Append a point if it isn’t a trivial duplicate; return True if appended."""
    lxz = _last_xz.get(name)
    xz = (float(x), float(z))
    if lxz is not None and _dxz(lxz, xz) <= MIN_DXZ:
        # Exact same X/Z as last seen (or under threshold): skip
        return False

    append_point(name, float(x), float(y), float(z), ts=ts, source=source, guild_id=guild_id)
    _last_xz[name] = xz
    log.debug(f"scanner: +point [{name}] @ ({x},{z}) via {source}")
    return True


async def ingest_line(guild_id: int, line: str, source: str, ts: datetime):
    """
    Called by log_fetcher for each accepted ADM line.
    Extracts {name, x,y,z} and pushes into the tracker.
    """
    m = RE_POS.search(line)
    if not m:
        m = RE_TP.search(line)
    if not m and ("performed" in line or "placed" in line or "teleport" in line):
        # Very permissive fallback if the line obviously describes an action with coords.
        m = RE_FALLBACK.search(line)

    if not m:
        return  # not a positional line we care about

    name = m.group("name").strip()
    try:
        x = float(m.group("x"))
        y = float(m.group("y"))
        z = float(m.group("z"))
    except Exception:
        return  # bad parse; ignore

    appended = _emit_point(name, x, y, z, ts=ts, source=source, guild_id=guild_id)
    if appended:
        # Optional: light INFO so you can see players are being captured
        log.info(f"Tracked [{name}] at ({x:.1f},{z:.1f}) from {source}")
