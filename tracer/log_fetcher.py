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

# Nitrado-style ADM names: DayZServer_X1_x64_YYYY-MM-DD_HH-MM-SS.ADM
ADM_NAME_TS = re.compile(
    r"dayzserver_x1_x64_(\d{4})[-_](\d{2})[-_](\d{2})[-_](\d{2})[-_](\d{2})[-_](\d{2})\.adm$",
    re.IGNORECASE,
)

def _parse_name_ts(name: str) -> datetime | None:
    m = ADM_NAME_TS.search(name)
    if not m:
        return None
    try:
        y, M, d, h, m, s = map(int, m.groups())
        return datetime(y, M, d, h, m, s, tzinfo=timezone.utc)
    except Exception:
        return None

def _ftp_latest_adm_with_mlsd(ftp: FTP, directory: str) -> str | None:
    """
    Try to use MLSD to find the newest .adm in `directory` by modify timestamp.
    Returns just the filename (not the full path), or None on failure / not found.
    Leaves CWD at `directory`.
    """
    try:
        ftp.cwd(directory)
        lines: list[str] = []
        ftp.retrlines("MLSD", lines.append)

        best_name: str | None = None
        best_modify: str | None = None  # YYYYMMDDHHMMSS

        for ln in lines:
            if " " not in ln:
                continue
            facts_part, name = ln.split(" ", 1)
            name = name.strip()
            if not name.lower().endswith(".adm"):
                continue

            facts = {}
            for kv in facts_part.split(";"):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    facts[k.lower()] = v

            if facts.get("type", "").lower() != "file":
                continue

            modify = facts.get("modify")
            if modify and (best_modify is None or modify > best_modify):
                best_modify = modify
                best_name = name

        return best_name
    except error_perm as e:
        logger.debug(f"MLSD not available in {directory}: {e}")
        return None
    except Exception as e:
        logger.debug(f"MLSD parse error in {directory}: {e}")
        return None

def _ftp_list_names(ftp: FTP, directory: str) -> list[str]:
    """
    NLST of `directory`, returns just names. Leaves CWD at `directory`.
    """
    ftp.cwd(directory)
    names: list[str] = []
    ftp.retrlines("NLST", names.append)
    return names

def _pick_latest_by_name(names: list[str]) -> str | None:
    """
    Prefer newest by parsed timestamp; if none parse, fall back to lexicographic.
    """
    adms = [n for n in names if n.lower().endswith(".adm")]
    if not adms:
        return None

    parsed = [(n, _parse_name_ts(n)) for n in adms]
    if any(ts is not None for _, ts in parsed):
        parsed = [(n, ts) for n, ts in parsed if ts is not None]
        parsed.sort(key=lambda x: x[1])  # ascending by timestamp
        return parsed[-1][0] if parsed else None

    # fallback: lexicographic usually matches the timestamped naming
    adms.sort()
    return adms[-1]

def _ftp_read_range_in_cwd(ftp: FTP, filename: str, start: int) -> bytes:
    """
    Read bytes of `filename` in the CURRENT directory from offset `start` to EOF.
    """
    bio = io.BytesIO()
    if start > 0:
        ftp.sendcmd(f"REST {start}")
    ftp.retrbinary(f"RETR {filename}", bio.write)
    return bio.getvalue()

async def _to_thread(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)

async def poll_guild(guild_id: int, cb: LineCallback, stop_event: asyncio.Event):
    """
    Poll FTP for a single guild. Reads only new bytes since last offset;
    if a newer ADM file appears, automatically switches to it.
    Robust to `adm_dir` being absolute (/games/…/dayzxb/config) or relative (dayzxb/config).
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

            # Always ensure we are in the configured directory before listing/reading
            try:
                await _to_thread(ftp.cwd, directory)
            except Exception as e:
                # Helpful diagnostics: show PWD and a root listing
                try:
                    pwd = await _to_thread(ftp.pwd)
                except Exception:
                    pwd = "(unknown)"
                logger.error(
                    f"[Guild {guild_id}] CWD to '{directory}' failed from PWD={pwd}: {e}",
                    exc_info=True,
                )
                # Try listing root to help the operator see what exists
                try:
                    root_names = await _to_thread(_ftp_list_names, ftp, "/")
                    logger.info(f"[Guild {guild_id}] FTP root entries: {root_names[:25]}")
                except Exception:
                    pass
                await _to_thread(ftp.quit)
                await asyncio.sleep(interval)
                continue

            # Prefer MLSD for freshest .ADM by mtime
            candidate = await _to_thread(_ftp_latest_adm_with_mlsd, ftp, ".")
            if not candidate:
                # Fallback to NLST + pick latest by parsed timestamp or lexicographic
                names = await _to_thread(_ftp_list_names, ftp, ".")
                candidate = _pick_latest_by_name(names)

            if not candidate:
                logger.debug(f"[Guild {guild_id}] No .ADM files found in {directory}")
                await _to_thread(ftp.quit)
                await asyncio.sleep(interval)
                continue

            if latest_file != candidate:
                logger.info(f"[Guild {guild_id}] Switching to new ADM file {candidate}")
                latest_file = candidate
                offset = 0
                set_guild_state(guild_id, latest_file=latest_file, offset=offset)

            # Ensure we are still in the directory, then read only the filename
            await _to_thread(ftp.cwd, directory)
            blob: bytes = await _to_thread(_ftp_read_range_in_cwd, ftp, latest_file, offset)
            await _to_thread(ftp.quit)

            if blob:
                text = blob.decode("utf-8", errors="ignore")
                now = datetime.now(timezone.utc)
                prev_offset = offset
                offset += len(blob)
                set_guild_state(guild_id, latest_file=latest_file, offset=offset)

                for idx, line in enumerate(text.splitlines()):
                    if buffer.accept(line):
                        source = f"ftp:{latest_file}#~{prev_offset}+{idx}"
                        await cb(guild_id, line, source, now)

        except Exception as e:
            logger.error(f"[Guild {guild_id}] FTP poll error: {e}", exc_info=True)

        await asyncio.sleep(interval)
