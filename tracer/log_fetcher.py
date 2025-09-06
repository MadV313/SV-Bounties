# tracer/log_fetcher.py
import asyncio
import io
import logging
import re
from ftplib import FTP, error_perm
from datetime import datetime, timezone
from typing import Callable, Awaitable, Optional

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

def _parse_name_ts(name: str) -> Optional[datetime]:
    m = ADM_NAME_TS.search(name)
    if not m:
        return None
    try:
        y, M, d, h, m, s = map(int, m.groups())
        return datetime(y, M, d, h, m, s, tzinfo=timezone.utc)
    except Exception:
        return None

def _ftp_mlsd_lines(ftp: FTP) -> list[str]:
    lines: list[str] = []
    ftp.retrlines("MLSD", lines.append)
    return lines

def _ftp_latest_adm_with_mlsd(ftp: FTP, directory: str) -> Optional[str]:
    """Use MLSD to find newest .adm by 'modify' fact. Leaves CWD at directory."""
    try:
        ftp.cwd(directory)
        lines = _ftp_mlsd_lines(ftp)
        best_name, best_modify = None, None  # type: Optional[str], Optional[str]
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
            modify = facts.get("modify")  # YYYYMMDDHHMMSS
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
    ftp.cwd(directory)
    names: list[str] = []
    ftp.retrlines("NLST", names.append)
    return names

def _pick_latest_by_name(names: list[str]) -> Optional[str]:
    adms = [n for n in names if n.lower().endswith(".adm")]
    if not adms:
        return None
    parsed = [(n, _parse_name_ts(n)) for n in adms]
    parsed = [(n, ts) for n, ts in parsed if ts is not None]
    if parsed:
        parsed.sort(key=lambda x: x[1])
        return parsed[-1][0]
    adms.sort()
    return adms[-1]

def _ftp_read_range_in_cwd(ftp: FTP, filename: str, start: int) -> bytes:
    """Read bytes of `filename` in CURRENT dir from offset `start` to EOF."""
    bio = io.BytesIO()
    if start > 0:
        # REST requires binary mode on some servers; we set TYPE I after login.
        ftp.sendcmd(f"REST {start}")
    ftp.retrbinary(f"RETR {filename}", bio.write)
    return bio.getvalue()

def _ftp_size(ftp: FTP, filename: str) -> Optional[int]:
    try:
        # SIZE works reliably in binary; TYPE I already set after login.
        resp = ftp.sendcmd(f"SIZE {filename}")
        # Expected like: "213 12345"
        parts = resp.split()
        if len(parts) >= 2 and parts[0] == "213":
            return int(parts[1])
    except Exception:
        pass
    return None

def _ftp_mdtm(ftp: FTP, filename: str) -> Optional[str]:
    try:
        resp = ftp.sendcmd(f"MDTM {filename}")  # "213 YYYYMMDDHHMMSS"
        parts = resp.split()
        if len(parts) >= 2 and parts[0] == "213":
            return parts[1]
    except Exception:
        pass
    return None

async def _to_thread(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)

async def poll_guild(guild_id: int, cb: LineCallback, stop_event: asyncio.Event):
    """
    Poll FTP for a single guild. Reads only new bytes since last offset;
    if a newer ADM file appears, automatically switches to it.
    Includes verbose heartbeat diagnostics so we can see exactly what's happening.
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
            ftp = await _to_thread(FTP, cfg["host"], timeout=25)
            # Passive + binary mode improves compatibility with REST/SIZE
            await _to_thread(ftp.login, cfg["username"], cfg["password"])
            try:
                await _to_thread(ftp.set_pasv, True)
            except Exception:
                pass
            try:
                await _to_thread(ftp.voidcmd, "TYPE I")
            except Exception:
                pass

            # Always try to enter the configured directory
            try:
                await _to_thread(ftp.cwd, directory)
            except Exception as e:
                # Heartbeat when CWD fails: show root PWD + a root NLST
                try:
                    pwd = await _to_thread(ftp.pwd)
                except Exception:
                    pwd = "(unknown)"
                logger.error(
                    f"[Guild {guild_id}] CWD to '{directory}' failed from PWD={pwd}: {e}",
                    exc_info=True,
                )
                try:
                    root_ls = await _to_thread(_ftp_list_names, ftp, "/")
                    logger.info(f"[Guild {guild_id}] FTP root entries: {root_ls[:40]}")
                except Exception:
                    pass
                await _to_thread(ftp.quit)
                await asyncio.sleep(interval)
                continue

            # Choose latest ADM
            candidate = await _to_thread(_ftp_latest_adm_with_mlsd, ftp, ".")
            if not candidate:
                names = await _to_thread(_ftp_list_names, ftp, ".")
                candidate = _pick_latest_by_name(names)

            if not candidate:
                logger.debug(f"[Guild {guild_id}] No .ADM files found in {directory}")
                await _to_thread(ftp.quit)
                await asyncio.sleep(interval)
                continue

            # If we switched files or current offset is past end, reset offset
            if latest_file != candidate:
                logger.info(f"[Guild {guild_id}] Switching to new ADM file {candidate}")
                latest_file = candidate
                offset = 0
                set_guild_state(guild_id, latest_file=latest_file, offset=offset)

            # Gather some heartbeat stats
            size = await _to_thread(_ftp_size, ftp, latest_file)
            mdtm = await _to_thread(_ftp_mdtm, ftp, latest_file)
            try:
                pwd_now = await _to_thread(ftp.pwd)
            except Exception:
                pwd_now = "(unknown)"

            # If saved offset > current file size, the file rolled/shrank -> reset
            if size is not None and offset > size:
                logger.info(
                    f"[Guild {guild_id}] Offset {offset} > size {size} for {latest_file}; resetting to 0 (rollover)."
                )
                offset = 0
                set_guild_state(guild_id, latest_file=latest_file, offset=offset)

            logger.debug(
                f"[Guild {guild_id}] HEARTBEAT: PWD={pwd_now} file={latest_file} size={size} mdtm={mdtm} offset={offset}"
            )

            # Read new bytes (if any)
            blob: bytes = await _to_thread(_ftp_read_range_in_cwd, ftp, latest_file, offset)
            await _to_thread(ftp.quit)

            if not blob:
                # No new data this tick; heartbeat at INFO so we can see it happening
                logger.info(
                    f"[Guild {guild_id}] No new bytes (file={latest_file} size={size} offset={offset}); waiting {interval}s."
                )
            else:
                text = blob.decode("utf-8", errors="ignore")
                now = datetime.now(timezone.utc)
                prev_offset = offset
                offset += len(blob)
                set_guild_state(guild_id, latest_file=latest_file, offset=offset)

                logger.info(
                    f"[Guild {guild_id}] Read {len(blob)} bytes from {latest_file} (prev_offset={prev_offset} -> {offset})."
                )

                for idx, line in enumerate(text.splitlines()):
                    if buffer.accept(line):
                        source = f"ftp:{latest_file}#~{prev_offset}+{idx}"
                        await cb(guild_id, line, source, now)

        except Exception as e:
            logger.error(f"[Guild {guild_id}] FTP poll error: {e}", exc_info=True)

        await asyncio.sleep(interval)
