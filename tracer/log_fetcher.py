# tracer/log_fetcher.py
import asyncio
import io
import logging
import os
import re
from collections import deque
from hashlib import blake2b
from ftplib import FTP, error_perm
from datetime import datetime, timezone
from typing import Callable, Awaitable, Optional, List, Tuple, Dict, Any

import requests  # <-- used only if Nitrado API keys are provided

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

# --- LIST fallback -----------------------------------------------------------
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
        parts = ln.split()
        if not parts:
            continue
        name = parts[-1]
        if name and name not in (".", ".."):
            names.append(name)
    return names
# ---------------------------------------------------------------------------

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


# ==================== Robust directory scan (FTP) ====================
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

# =================== Nitrado API discovery (optional) ===================
def _nitrado_api_get_latest(cfg: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (filename, download_url) for the newest ADM via Nitrado HTTP API.
    Uses either cfg[...] or environment variables:
      - nitrado_api_token / NITRADO_API_TOKEN
      - nitrado_service_id / NITRADO_SERVICE_ID
      - nitrado_log_folder_prefix / NITRADO_LOG_DIR
    If anything is missing, returns (None, None).
    """
    token = (cfg.get("nitrado_api_token")
             or os.getenv("NITRADO_API_TOKEN"))
    service_id = (cfg.get("nitrado_service_id")
                  or os.getenv("NITRADO_SERVICE_ID"))
    dir_path = (cfg.get("nitrado_log_folder_prefix")
                or os.getenv("NITRADO_LOG_DIR"))

    if not token or not service_id or not dir_path:
        return (None, None)

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        list_url = f"https://api.nitrado.net/services/{service_id}/gameservers/file_server/list"
        r = requests.get(list_url, headers=headers, params={"dir": dir_path}, timeout=10)
        if r.status_code != 200:
            logger.info(f"[NitradoAPI] list failed HTTP {r.status_code}")
            return (None, None)
        entries = r.json().get("data", {}).get("entries", []) or []
        adm = [e for e in entries if str(e.get("name","")).lower().endswith(".adm")]
        if not adm:
            logger.info("[NitradoAPI] no ADM entries in listing")
            return (None, None)
        # newest by filename timestamp
        def _dt(e):
            return _parse_name_ts(e.get("name","")) or datetime.min.replace(tzinfo=timezone.utc)
        latest = max(adm, key=_dt)
        fname = latest.get("name")
        # fetch download token
        down_url = f"https://api.nitrado.net/services/{service_id}/gameservers/file_server/download"
        r2 = requests.get(down_url, headers=headers,
                          params={"file": f"{dir_path.rstrip('/')}/{fname}"}, timeout=10)
        if r2.status_code != 200:
            logger.info(f"[NitradoAPI] download token failed HTTP {r2.status_code}")
            return (None, None)
        url = r2.json().get("data", {}).get("token", {}).get("url")
        if not url:
            return (None, None)
        return (fname, url)
    except Exception as e:
        logger.info(f"[NitradoAPI] error: {e}")
        return (None, None)
# =====================================================================


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
    - Unified FTP listing (MLSD ∪ NLST ∪ LIST) + candidate logging
    - **NEW**: Nitrado API discovery of the newest ADM (active file), with HTTP
               download fallback when FTP cannot read it.
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

            # ===== API discovery (optional). If available and newer, prefer it.
            api_name, api_download_url = await _to_thread(_nitrado_api_get_latest, cfg)
            api_dt = _parse_name_ts(api_name or "") if api_name else None

            if not files and not api_name:
                logger.debug(f"[Guild {guild_id}] No .ADM files found; PWD={pwd_now}")
                logger.info(f"[Guild {guild_id}] NLST sample: {raw_nlst[:20]}")
                logger.info(f"[Guild {guild_id}] LIST sample: {raw_list[:20]}")
                await _to_thread(ftp.quit)
                if last_seen_hash is not None:
                    logger.info(f"[Guild {guild_id}] Last line hash #{last_seen_hash}: {last_seen_line[:160]}")
                await asyncio.sleep(interval)
                continue

            latest_name, latest_size_guess, latest_mtime = (None, 0, None)
            if files:
                latest_name, latest_size_guess, latest_mtime = _choose_latest_adm(files)

            # Compare FTP newest vs API newest
            chosen_name = latest_name
            chosen_mtime = latest_mtime
            chosen_api_url = None

            if api_name:
                if not chosen_name:
                    chosen_name = api_name
                    chosen_mtime = api_dt
                    chosen_api_url = api_download_url
                else:
                    # pick newer by timestamp
                    ftp_dt = chosen_mtime or _parse_name_ts(chosen_name) or datetime.min.replace(tzinfo=timezone.utc)
                    if api_dt and api_dt > ftp_dt:
                        logger.info(
                            f"[Guild {guild_id}] API discovered newer ADM: {api_name} "
                            f"(API {api_dt.isoformat()} > FTP {ftp_dt.isoformat()})"
                        )
                        chosen_name = api_name
                        chosen_mtime = api_dt
                        chosen_api_url = api_download_url

            # Candidate table (old→new, last few entries)
            if files:
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

            if api_name:
                logger.info(f"[Guild {guild_id}] API latest hint: {api_name}")

            # Nothing chosen? Bail.
            if not chosen_name:
                await _to_thread(ftp.quit)
                await asyncio.sleep(interval)
                continue

            # Switch if changed
            if latest_file != chosen_name:
                logger.info(
                    f"[Guild {guild_id}] Switching ADM {latest_file or '<none>'} → {chosen_name}"
                )
                latest_file = chosen_name
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

            # Try ranged read via FTP
            ftp_failed = False
            try:
                blob: bytes = await _to_thread(_ftp_read_range_in_cwd, ftp, latest_file, offset)
            except Exception as e:
                ftp_failed = True
                logger.info(f"[Guild {guild_id}] FTP RETR failed for {latest_file}: {e}")
                blob = b""

            # If no data from FTP, consider HTTP download fallback (API)
            full_fetch_used = False
            http_size = None
            if (not blob) and chosen_api_url:
                try:
                    r = await _to_thread(requests.get, chosen_api_url, )
                    if r.status_code == 200 and r.content is not None:
                        http_bytes = r.content
                        http_size = len(http_bytes)
                        # Only process new tail (prev offset .. end)
                        data_to_process = http_bytes[offset:] if offset < http_size else b""
                        blob = data_to_process
                        full_fetch_used = True
                        size = http_size  # for logging/offset update below
                        logger.info(
                            f"[Guild {guild_id}] HTTP fallback used for {latest_file} "
                            f"(downloaded {http_size} bytes, tail={len(blob)} from offset {offset})."
                        )
                    else:
                        logger.info(f"[Guild {guild_id}] HTTP fallback failed HTTP {r.status_code}")
                except Exception as e:
                    logger.info(f"[Guild {guild_id}] HTTP fallback error: {e}")

            await _to_thread(ftp.quit)

            if not blob:
                logger.info(
                    f"[Guild {guild_id}] No new bytes (file={latest_file} size={size} offset={offset}); waiting {interval}s."
                )
            else:
                prev_offset = offset

                # If we did an HTTP full-file fetch, offset should become http_size
                if full_fetch_used and http_size is not None:
                    offset = http_size
                else:
                    # Standard FTP increment (we fetched from 'offset' to EOF)
                    offset += len(blob)

                set_guild_state(guild_id, latest_file=latest_file, offset=offset)
                logger.info(
                    f"[Guild {guild_id}] Read {len(blob)} bytes from {latest_file} (prev_offset={prev_offset} -> {offset})."
                )

                text = blob.decode("utf-8", errors="ignore")
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
