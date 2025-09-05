# tracer/scanner.py
import re
from datetime import datetime
from tracer.tracker import append_point

PLAYER_NAME = r'(?:Player\s+"([^"]+)"|([A-Za-z0-9_]+))'
POS = r'pos=<\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*>'
GENERIC_POS_LINE = re.compile(PLAYER_NAME + r'.*?' + POS)

async def scan_adm_line(guild_id: int, line: str, source_ref: str, ts: datetime):
    """
    Called for every new ADM line discovered by the log fetcher.
    """
    m = GENERIC_POS_LINE.search(line)
    if not m:
        return
    gamertag = m.group(1) or m.group(2)
    x, y, z = float(m.group(3)), float(m.group(4)), float(m.group(5))
    append_point(gamertag=gamertag, x=x, y=y, z=z, ts=ts, source=source_ref)
