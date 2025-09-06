# tracer/scanner.py
import re
import logging
from datetime import datetime
from typing import Optional, Tuple, Dict

from tracer.tracker import append_point

log = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Patterns to extract player name + coordinates from DayZ ADM lines
# --------------------------------------------------------------------

# Common "pos=<x,y,z>" lines:
#   15:44:16 | Player "SoulTatted94" (...) pos=<5188.7, 10319.5, 191.2> ...
RE_POS = re.compile(
    r'Player\s+"(?P<name>[^"]+)"[^<]*?pos=<\s*(?P<x>-?\d+(?:\.\d+)?),\s*(?P<y>-?\d+(?:\.\d+)?),\s*(?P<z>-?\d+(?:\.\d+)?)\s*>',
    re.IGNORECASE,
)

# Teleport lines — record the *destination* coords:
#   ... Player "Foo" ... was teleported from: <...> to: <x,y,z>
RE_TP = re.compile(
    r'Player\s+"(?P<name>[^"]+)"[^<]*?teleport[^:]*?:.*?to:\s*<\s*(?P<x>-?\d+(?:\.\d+)?),\s*(?P<y>-?\d+(?:\.\d+)?),\s*(?P<z>-?\d+(?:\.\d+)?)\s*>',
    re.IGNORECASE,
)

# Fallback: sometimes action lines include a bare "<x,y,z>" after the name.
# Only used for lines that clearly describe an action to reduce false positives.
RE_FALLBACK = re.compile(
    r'Player\s+"(?P<name>[^"]+)"[^\n]*?<\s*(?P<x>-?\d+(?:\.\d+)?),\s*(?P<y>-?\d+(?:\.\d+)?),\s*(?P<z>-?\d+(?:\.\d+)?)\s*>',
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

def _emit_point(
    name: str,
    x: float,
    y: float,
    z: float,
    ts: datetime,
    source: str,
    guild_id: Optional[int],
) -> bool:
    """Append to the tracker if not a trivial duplicate; True if appended."""
    xz = (float(x), float(z))
    last = _last_xz.get(name)
    if last is not None and _dxz(last, xz) <= MIN_DXZ:
        return False

    append_point(name, float(x), float(y), float(z), ts=ts, source=source, guild_id=guild_id)
    _last_xz[name] = xz
    log.debug(f"scanner: +point [{name}] @ ({x},{z}) via {source}")
    return True


async def scan_adm_line(guild_id: int, line: str, source_ref: str, timestamp: datetime):
    """
    Entry point used by the poller (signature matches LineCallback).
    Extracts {name,x,y,z} from ADM lines and forwards to tracker.
    """
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
        y = float(m.group("y"))
        z = float(m.group("z"))
    except Exception:
        return  # Parse failure; ignore.

    if _emit_point(name, x, y, z, ts=timestamp, source=source_ref, guild_id=guild_id):
        # Light INFO so you can confirm players are being captured.
        log.info(f"Tracked [{name}] at ({x:.1f},{z:.1f}) from {source_ref}")


# Backwards-compatible alias (if other code imports a different name)
ingest_line = scan_adm_line
