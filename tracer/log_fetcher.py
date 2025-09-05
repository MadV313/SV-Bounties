# tracer/log_fetcher.py
import asyncio
import io
import logging
import re
from ftplib import FTP, error_perm
from datetime import datetime, timezone
from typing import Callable, Awaitable

from utils.ftp_config import get_ftp_config
from tracer.adm_state import get_guild_state, set_guild_state
from tracer.adm_buffer import AdmBuffer

logger = logging.getLogger(__name__)

LineCallback = Callable[[int, str, str, datetime], Awaitable[None]]
# signature: (guild_id, line, source_ref, timestamp)

# Nitrado ADM filename format:
# DayZServer_X1_x64_2025-09-05_12-14-31.ADM (dashes or underscores between parts)
ADM_PATTERN = re.compile(
    r"DayZServer_X1_x64_(\d{4})[-_](\d{2})[-_](\d{2})[-_](\d{2})[-_](\d{2})[-_](\d{2})\.ADM",
    re.IGNORECASE,
)

def _extract_timestamp_from_name(name: str) -> datetime | None:
    m = ADM_PATTERN.match(name)
    if not m:
        return None
    try:
        y, mo, d, h, mi, s = map(int, m.groups())
        return datetime(y, mo, d, h, mi, s)
    except ValueError:
        return None


def _ftp_list_names(ftp: FTP, directory: str) -> list[str]:
    """NLST names only for a directory (no facts)."""
    ftp.cwd(directory)
    names: list[str] = []
    ftp.retrlines("NLST", names.append)
    return names


def _latest_adm_by_filename(names: list[str]) -> str | None:
    """
    Choose newest ADM by parsing the timestamp embedded in the Nitrado filename.
    Returns the filename or None if no parseable .ADM is found.
    """
    pairs = []
    for n in names:
        if n.lower().endswith(".adm"):
            ts = _extract_timestamp_from_name(n)
            if ts:
                pairs.append((n, ts))
    if not pairs:
        return None
    # pick max by timestamp
    return max(pairs, key=lambda x: x[1])[0]


def _ftp_latest_adm_with_mlsd(ftp: FTP, directory: str) -> str | None:
    """
    Try to use MLSD to find the newest .adm in `directory` by modify timestamp.
    Returns just the filename (not the full path), or None on failure / not found.
    """
    try:
        ftp.cwd(directory)
        lines: list[str] = []
        # Some servers may not support MLSD -> will raise error_perm
        ftp.retrlines("MLSD", lines.append)

        latest_name: str | None = None
        latest_modify: str | None = None  # YYYYMMDDHHMMSS

        for ln in lines:
            # MLSD line example:
            # "type=file;size=1234;modify=20250905140246;UNIX.mode=0644; ... filename.ext"
            if " " not in ln:
                continue
            facts_part, name = ln.split(" ", 1)
            name = name.strip()
            if not name.lower().endswith(".adm"):
                continue

            # collect facts into dict
            facts = {}
            for kv in facts_part.split(";"):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    facts[k.lower()] = v

            if facts.get("type", "").lower() != "file":
                continue

            modify = facts.get("modify")  # e.g. "20250905140246"
            if modify and (latest_modify is None or modify > latest_modify):
                latest_modify = modify
                latest_name = name

        return latest_name
    except error_perm as e:
        # MLSD not supported or permission denied; caller will fall back
        logger.debug(f"MLSD not available in {directory}: {e}")
        return None
    except Exception as e:
        logger.debug(f"MLSD parse error in {directory}: {e}")
        return None


def _ftp_latest_adm_by_name(ftp: FTP, directory: str) -> str | None:
    """
    Fallback: list names only and pick the last .adm after lexicographic sort.
    """
    names = _ftp_list_names(ftp, directory)
    names = sorted(n for n in names if n.lower().endswith(".adm"))
    return names[-1] if names else None


def _ftp_read_range(ftp: FTP, path: str, start: int) -> bytes:
    """Read from byte offset `start` to EOF using REST+RETR."""
    bio = io.BytesIO()
    if start > 0:
        ftp.sendcmd(f"REST {start}")
    ftp.retrbinary(f"RETR {path}", bio.write)
    return bio.getvalue()


async def _to_thread(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


async def poll_guild(guild_id: int, cb: LineCallback, stop_event: asyncio.Event):
    """
    Poll FTP for a single guild. Reads only new bytes since last offset;
    if a newer ADM file appears, automatically switches to it.
    """
    cfg = get_ftp_config(guild_id)
    if not cfg:
        logger.warning(f"[Guild {guild_id}] No FTP config set; skipping poller.")
        return

    interval = max(5, int(cfg.get("interval_sec", 10)))
    directory = cfg.get("adm_dir", "/")

    buffer = AdmBuffer(max_remember=200)
    state = get_guild_state(guild_id)
    latest_file = state.get("latest_file")
    offset = int(state.get("offset") or 0)

    logger.info(f"[Guild {guild_id}] Starting ADM poller (dir={directory}, every {interval}s).")

    while not stop_event.is_set():
        try:
            # Connect each cycle (resilient to idle timeouts)
            ftp = await _to_thread(FTP, cfg["host"], timeout=20)
            await _to_thread(ftp.login, cfg["username"], cfg["password"])

            # 1) Prefer newest-by-filename (parse the timestamp)
            names = await _to_thread(_ftp_list_names, ftp, directory)
            candidate = _latest_adm_by_filename(names)

            # 2) If none matched pattern, try MLSD by mtime
            if not candidate:
                candidate = await _to_thread(_ftp_latest_adm_with_mlsd, ftp, directory)

            # 3) Final fallback: lexicographic last
            if not candidate:
                candidate = await _to_thread(_ftp_latest_adm_by_name, ftp, directory)

            if not candidate:
                logger.debug(f"[Guild {guild_id}] No .ADM files found in {directory}")
                await _to_thread(ftp.quit)
                await asyncio.sleep(interval)
                continue

            path = f"{directory.rstrip('/')}/{candidate}"

            if latest_file != candidate:
                logger.info(f"[Guild {guild_id}] Switching to new ADM file {candidate}")
                latest_file = candidate
                offset = 0
                set_guild_state(guild_id, latest_file=latest_file, offset=offset)

            blob: bytes = await _to_thread(_ftp_read_range, ftp, path, offset)
            await _to_thread(ftp.quit)

            if blob:
                text = blob.decode("utf-8", errors="ignore")
                now = datetime.now(timezone.utc)
                prev_offset = offset
                offset += len(blob)
                set_guild_state(guild_id, latest_file=latest_file, offset=offset)

                # Feed new lines to callback
                for idx, line in enumerate(text.splitlines()):
                    if buffer.accept(line):
                        source = f"ftp:{candidate}#~{prev_offset}+{idx}"
                        await cb(guild_id, line, source, now)

        except Exception as e:
            # Log and keep polling
            logger.error(f"[Guild {guild_id}] FTP poll error: {e}", exc_info=True)

        await asyncio.sleep(interval)
