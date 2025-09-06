# tracer/log_fetcher.py
import asyncio
import io
import logging
import re
from collections import deque
from hashlib import blake2b
from ftplib import FTP, error_perm
from datetime import datetime, timezone
from typing import Callable, Awaitable, Optional, List, Tuple, Dict, Any

from utils.ftp_config import get_ftp_config
from tracer.adm_state import get_guild_state, set_guild_state
from tracer.adm_buffer import AdmBuffer

logger = logging.getLogger(__name__)

LineCallback = Callable[[int, str, str, datetime], Awaitable[None]]
# signature: (guild_id, line, source_ref, timestamp)

# --- small time helper -------------------------------------------------------
def _when() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
# -----------------------------------------------------------------------------

# --- HASH DE-DUPE (Radar-style) ---------------------------------------------
# Rolling set of recent line fingerprints so “bursty” additions are never missed
# even if FTP size/offset behavior glitches.
MAX_SEEN_HASHES = 4000  # ~the last few thousand lines is plenty

def _line_fingerprint(s: str) -> int:
    # Stable, fast 64-bit fingerprint; ignore trailing whitespace to be robust
    # to \r\n vs \n and minor EOL differences.
    h = blake2b(s.rstrip().encode("utf-8", "ignore"), digest_size=8)
    return int.from_bytes(h.digest(), "big")
# -----------------------------------------------------------------------------


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

# --- NEW: LIST fallback ------------------------------------------------------
def _ftp_list_via_LIST(ftp: FTP, directory: str) -> list[str]:
    """
    Fallback: parse plain LIST lines to get filenames.
    Some servers show the newest file here even when MLSD/NLST hide it briefly.
    """
    ftp.cwd(directory)
    raw: list[str] = []
    ftp.retrlines("LIST", raw.append)
    names: list[str] = []
    for ln in raw:
        # Typical: "-rw-r--r--   1 0 0   12345 Sep  6 09:03 DayZServer_X1_x64_2025-09-06_09-03-04.ADM"
        parts = ln.split()
        if not parts:
            continue
        name = parts[-1]
        if name and name not in (".", ".."):
            names.append(name)
    return names
# -----------------------------------------------------------------------------

def _pick_latest_by_name(names: list[str]) -> Optional[str]:
    adms = [n for n in names if n.lower().endswith(".adm")]
    if not adms:
        return None
    parsed = [(n, _parse_name_ts(n)) for n in adms]
    parsed = [(n, ts) for n, ts in parsed if ts is not None]
    if parsed:
        parsed.sort(key=lambda x: x[1])  # ascending by timestamp
        return parsed[-1][0]
    adms.sort()
    return adms[-1]

def _ensure_binary(ftp: FTP) -> None:
    try:
        ftp.voidcmd("TYPE I")
    except Exception:
        pass

def _ftp_read_range_in_cwd(ftp: FTP, filename: str, start: int) -> bytes:
    """
    Read bytes of `filename` in CURRENT dir from offset `start` to EOF.
    Ensures binary mode (TYPE I) so REST works on Nitrado.
    Retries once if the server rejects REST due to ASCII mode.
    """
    bio = io.BytesIO()
    _ensure_binary(ftp)

    if start > 0:
        try:
            ftp.sendcmd(f"REST {start}")
        except error_perm as e:
            # Typical message: "501 REST: Resuming transfers not allowed in ASCII mode"
            msg = str(e)
            if "501" in msg or "ascii" in msg.lower():
                try:
                    _ensure_binary(ftp)
                    ftp.sendcmd(f"REST {start}")
                except Exception:
                    # Signal caller; they may try bounded full-file fallback
                    return b""
            else:
                raise

    ftp.retrbinary(f"RETR {filename}", bio.write)
    return bio.getvalue()

def _ftp_read_all_in_cwd(ftp: FTP, filename: str) -> bytes:
    """Fetch entire file (binary). Use sparingly as fallback."""
    bio = io.BytesIO()
    _ensure_binary(ftp)
    ftp.retrbinary(f"RETR {filename}", bio.write)
    return bio.getvalue()

def _ftp_size(ftp: FTP, filename: str) -> Optional[int]:
    try:
        resp = ftp.sendcmd(f"SIZE {filename}")  # "213 12345"
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


# ==================== NEW: robust directory scan ====================
def _list_adm_files(ftp: FTP) -> List[Tuple[str, int, Optional[datetime]]]:
    """
    Return (name, size, mtime) for each *.ADM in the CWD.
    We UNION the results of MLSD, NLST, and LIST to avoid transient omissions.
    """
    out: Dict[str, Tuple[str, int, Optional[datetime]]] = {}

    # MLSD pass
    try:
        for name, facts in list(ftp.mlsd()):
            if not name.lower().endswith(".adm"):
                continue
            if facts.get("type", "").lower() != "file":
                continue
            size = int(facts.get("size", "0"))
            mtime = None
            mod = facts.get("modify")
            if mod and len(mod) >= 14:
                try:
                    mtime = datetime.strptime(mod[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                except Exception:
                    mtime = None
            if mtime is None:
                mtime = _parse_name_ts(name)
            out[name] = (name, size, mtime)
    except Exception:
        pass

    # NLST pass
    try:
        names: List[str] = []
        ftp.retrlines("NLST", names.append)
        for n in names:
            if not n.lower().endswith(".adm"):
                continue
            try:
                size = ftp.size(n) or 0
            except Exception:
                size = 0
            mt = out.get(n, (None, 0, None))[2] or _parse_name_ts(n)
            out.setdefault(n, (n, size, mt))
    except Exception:
        pass

    # LIST pass (last resort for hidden newest)
    try:
        raw: List[str] = []
        ftp.retrlines("LIST", raw.append)
        for ln in raw:
            parts = ln.split()
            if not parts:
                continue
            n = parts[-1]
            if not n.lower().endswith(".adm"):
                continue
            if n not in out:
                try:
                    size = ftp.size(n) or 0
                except Exception:
                    size = 0
                out[n] = (n, size, _parse_name_ts(n))
    except Exception:
        pass

    return list(out.values())


def _choose_latest_adm(files: List[Tuple[str, int, Optional[datetime]]]) -> Tuple[str, int, Optional[datetime]]:
    """
    Choose the newest file by mtime (or filename timestamp), returning (name, size, mtime).
    """
    if not files:
        raise ValueError("No ADM files")
    def key(row):
        name, size, mtime = row
        t = mtime or _parse_name_ts(name) or datetime.min.replace(tzinfo=timezone.utc)
        return (t, name)
    name, size, mtime = max(files, key=key)
    return name, size, mtime
# ===================================================================


async def _to_thread(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)

async def poll_guild(guild_id: int, cb: LineCallback, stop_event: asyncio.Event):
    """
    Poll FTP for a single guild. Reads new bytes since last offset; if a newer
    ADM file appears, automatically switches to it.

    Improvements:
    - Radar-style line-hash de-dupe (never miss short bursts; skip replays)
    - Bounded full-file fallback when REST fails (<= 512 KiB)
    - Heartbeat diagnostics (size/mdtm/offset)
    - Last-line hashed print every tick
    - NEW: unified listing (MLSD ∪ NLST ∪ LIST) + candidate logging
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

    # De-dupe cache (rolling) for this poller
    seen_set: set[int] = set()
    seen_queue: deque[int] = deque()

    # Last accepted line (for the hash print each tick)
    last_seen_line: Optional[str] = None
    last_seen_hash: Optional[int] = None

    def _remember_line(line: str) -> bool:
        """Returns True if this line is new (not seen recently), and updates last-line state."""
        nonlocal last_seen_line, last_seen_hash
        fp = _line_fingerprint(line)
        if fp in seen_set:
            return False
        seen_set.add(fp)
        seen_queue.append(fp)
        if len(seen_queue) > MAX_SEEN_HASHES:
            old = seen_queue.popleft()
            seen_set.discard(old)
        last_seen_line = line.rstrip()
        last_seen_hash = fp
        return True

    logger.info(f"[Guild {guild_id}] Starting ADM poller (dir={directory}, every {interval}s).")

    while not stop_event.is_set():
        try:
            ftp = await _to_thread(FTP, cfg["host"], timeout=25)
            # Login, passive, and binary mode for REST/SIZE
            await _to_thread(ftp.login, cfg["username"], cfg["password"])
            try:
                await _to_thread(ftp.set_pasv, True)
            except Exception:
                pass
            try:
                await _to_thread(ftp.voidcmd, "TYPE I")
            except Exception:
                pass

            # Enter the configured directory
            try:
                await _to_thread(ftp.cwd, directory)
            except Exception as e:
                # Diagnostics when CWD fails
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
                # --- Last-line hash print (tick) ---
                if last_seen_hash is not None:
                    logger.info(f"[Guild {guild_id}] Last line hash #{last_seen_hash}: {last_seen_line[:160]}")
                await asyncio.sleep(interval)
                continue

            # ===== unified directory scan (MLSD ∪ NLST ∪ LIST)
            files = await _to_thread(_list_adm_files, ftp)
            try:
                pwd_now = await _to_thread(ftp.pwd)
            except Exception:
                pwd_now = "(unknown)"

            # Extra diagnostics to catch omissions
            try:
                raw_nlst = await _to_thread(_ftp_list_names, ftp, ".")
            except Exception:
                raw_nlst = []
            try:
                raw_list = await _to_thread(_ftp_list_via_LIST, ftp, ".")
            except Exception:
                raw_list = []

            if not files:
                logger.debug(f"[Guild {guild_id}] No .ADM files found; PWD={pwd_now}")
                logger.info(f"[Guild {guild_id}] NLST sample: {raw_nlst[:20]}")
                logger.info(f"[Guild {guild_id}] LIST sample: {raw_list[:20]}")
                await _to_thread(ftp.quit)
                if last_seen_hash is not None:
                    logger.info(f"[Guild {guild_id}] Last line hash #{last_seen_hash}: {last_seen_line[:160]}")
                await asyncio.sleep(interval)
                continue

            latest_name, latest_size_guess, latest_mtime = _choose_latest_adm(files)

            # Candidate table (old→new, last few entries)
            pretty = [
                {
                    "name": n,
                    "size": s,
                    "mtime": (mt.isoformat() if mt else None),
                }
                for n, s, mt in sorted(files, key=lambda r: (r[2] or _parse_name_ts(r[0]) or datetime.min))
            ]
            logger.info(f"[Guild {guild_id}] PWD={pwd_now}")
            logger.info(f"[Guild {guild_id}] ADM candidates (old→new): {pretty[-6:]}")
            logger.debug(f"[Guild {guild_id}] NLST raw (trim): {raw_nlst[-10:]}")
            logger.debug(f"[Guild {guild_id}] LIST raw (trim): {raw_list[-10:]}")

            # Switch if changed
            if latest_file != latest_name:
                logger.info(
                    f"[Guild {guild_id}] Switching ADM {latest_file or '<none>'} → {latest_name}"
                )
                latest_file = latest_name
                offset = 0
                set_guild_state(guild_id, latest_file=latest_file, offset=offset)

            # Heartbeat stats for current file
            size = await _to_thread(_ftp_size, ftp, latest_file)
            mdtm = await _to_thread(_ftp_mdtm, ftp, latest_file)

            if size is not None and offset > size:
                logger.info(
                    f"[Guild {guild_id}] Offset {offset} > size {size} for {latest_file}; resetting to 0 (rollover/truncation)."
                )
                offset = 0
                set_guild_state(guild_id, latest_file=latest_file, offset=offset)

            logger.debug(
                f"[Guild {guild_id}] HEARTBEAT: file={latest_file} size={size} mdtm={mdtm} offset={offset}"
            )

            # Extra guard: if another file is strictly newer than our current MDTM, switch now
            try:
                current_dt = None
                if mdtm and len(mdtm) >= 14:
                    current_dt = datetime.strptime(mdtm[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                else:
                    current_dt = _parse_name_ts(latest_file or "")
            except Exception:
                current_dt = _parse_name_ts(latest_file or "")

            if latest_mtime and current_dt and latest_mtime > current_dt and latest_name != latest_file:
                logger.info(
                    f"[Guild {guild_id}] Detected newer ADM on server "
                    f"({latest_name} @ {latest_mtime.isoformat()}) > "
                    f"({latest_file} @ {current_dt.isoformat()}); forcing switch."
                )
                latest_file = latest_name
                offset = 0
                set_guild_state(guild_id, latest_file=latest_file, offset=offset)
                # refresh size for new file
                size = await _to_thread(_ftp_size, ftp, latest_file)

            # Try ranged read
            blob: bytes = await _to_thread(_ftp_read_range_in_cwd, ftp, latest_file, offset)

            # If no data but file grew, do a bounded full-file fallback (<= 512 KiB)
            full_fetch_used = False
            if not blob and size is not None and size > offset and size <= 512_000:
                logger.info(
                    f"[Guild {guild_id}] Range read empty but file grew (size={size} > offset={offset}); "
                    "attempting bounded full-file fetch."
                )
                blob = await _to_thread(_ftp_read_all_in_cwd, ftp, latest_file)
                full_fetch_used = True

            await _to_thread(ftp.quit)

            if not blob:
                logger.info(
                    f"[Guild {guild_id}] No new bytes (file={latest_file} size={size} offset={offset}); waiting {interval}s."
                )
            else:
                prev_offset = offset

                # If we did a full-file fetch and know the size, only process the new tail
                data_to_process = blob
                if full_fetch_used and size is not None:
                    if prev_offset < size and prev_offset < len(blob):
                        data_to_process = blob[prev_offset:]
                    offset = size
                else:
                    offset += len(blob)

                set_guild_state(guild_id, latest_file=latest_file, offset=offset)
                logger.info(
                    f"[Guild {guild_id}] Read {len(blob)} bytes from {latest_file} (prev_offset={prev_offset} -> {offset})."
                )

                text = data_to_process.decode("utf-8", errors="ignore")
                now = datetime.now(timezone.utc)

                for idx, line in enumerate(text.splitlines()):
                    # 1) radar-style recent-hash de-dupe
                    if not _remember_line(line):
                        continue
                    # 2) your existing acceptance filter
                    if buffer.accept(line):
                        source = f"ftp:{latest_file}#~{prev_offset}+{idx}"
                        await cb(guild_id, line, source, now)

        except Exception as e:
            logger.error(f"[Guild {guild_id}] FTP poll error: {e}", exc_info=True)

        # --- Last-line hash print (every tick) ---
        if last_seen_hash is not None:
            logger.info(f"[Guild {guild_id}] Last line hash #{last_seen_hash}: {last_seen_line[:160]}")

        await asyncio.sleep(interval)
