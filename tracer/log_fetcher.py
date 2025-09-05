# tracer/log_fetcher.py
import asyncio
import io
from ftplib import FTP
from datetime import datetime, timezone
from typing import Callable, Awaitable, Optional

from utils.ftp_config import get_ftp_config
from tracer.adm_state import get_guild_state, set_guild_state
from tracer.adm_buffer import AdmBuffer

LineCallback = Callable[[int, str, str, datetime], Awaitable[None]]
# signature: (guild_id, line, source_ref, timestamp)

def _ftp_list_files(ftp: FTP, directory: str) -> list[str]:
    files = []
    ftp.cwd(directory)
    ftp.retrlines("NLST", files.append)  # names only
    return sorted(files)  # lexicographic is usually by timestamp in ADM naming

def _ftp_read_range(ftp: FTP, path: str, start: int) -> bytes:
    # Read from byte offset 'start' to EOF using REST
    bio = io.BytesIO()
    # REST sets offset for next RETR
    if start > 0:
        ftp.sendcmd(f"REST {start}")
    ftp.retrbinary(f"RETR {path}", bio.write)
    return bio.getvalue()

async def _to_thread(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)

async def poll_guild(guild_id: int, cb: LineCallback, stop_event: asyncio.Event):
    """
    Polls FTP for a single guild. Reads only new bytes since last offset; if a new
    latest ADM file appears, automatically switches to it.
    """
    cfg = get_ftp_config(guild_id)
    if not cfg:
        return  # not configured for this guild

    interval = max(5, int(cfg.get("interval_sec", 10)))
    directory = cfg.get("adm_dir", "/")

    buffer = AdmBuffer(max_remember=200)
    latest_file = get_guild_state(guild_id).get("latest_file")
    offset = int(get_guild_state(guild_id).get("offset") or 0)

    while not stop_event.is_set():
        try:
            # Connect each cycle (simpler and resilient to idle timeouts)
            ftp = await _to_thread(FTP, cfg["host"], timeout=20)
            await _to_thread(ftp.login, cfg["username"], cfg["password"])
            files = await _to_thread(_ftp_list_files, ftp, directory)
            # Filter to likely ADM files
            files = [f for f in files if f.lower().endswith((".adm", ".log", ".txt"))]
            if not files:
                await _to_thread(ftp.quit)
                await asyncio.sleep(interval)
                continue

            candidate = files[-1]  # pick latest by name
            path = f"{directory.rstrip('/')}/{candidate}"

            if latest_file != candidate:
                # new file rolled over
                latest_file = candidate
                offset = 0
                set_guild_state(guild_id, latest_file=latest_file, offset=offset)

            blob: bytes = await _to_thread(_ftp_read_range, ftp, path, offset)
            await _to_thread(ftp.quit)

            if blob:
                text = blob.decode("utf-8", errors="ignore")
                now = datetime.now(timezone.utc)
                # Update offset first so even if downstream crashes we won't repeat
                offset += len(blob)
                set_guild_state(guild_id, latest_file=latest_file, offset=offset)

                # Feed new lines to callback
                for idx, line in enumerate(text.splitlines()):
                    if buffer.accept(line):
                        source = f"ftp:{candidate}#~{offset - len(blob)}+{idx}"
                        await cb(guild_id, line, source, now)
        except Exception:
            # swallow exceptions and keep polling; you can add logging here
            pass

        await asyncio.sleep(interval)
