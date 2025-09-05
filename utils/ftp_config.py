# utils/ftp_config.py
import json
from pathlib import Path
from typing import Optional

FTP_STORE = "data/ftp_config.json"

def _load() -> dict:
    p = Path(FTP_STORE)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save(obj: dict):
    p = Path(FTP_STORE)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def set_ftp_config(guild_id: int, host: str, username: str, password: str,
                   port: int = 21, adm_dir: str = "/", interval_sec: int = 10):
    data = _load()
    data[str(guild_id)] = {
        "host": host, "port": port, "username": username,
        "password": password, "adm_dir": adm_dir,
        "interval_sec": interval_sec
    }
    _save(data)

def get_ftp_config(guild_id: int) -> Optional[dict]:
    return _load().get(str(guild_id))

def clear_ftp_config(guild_id: int):
    data = _load()
    if str(guild_id) in data:
        del data[str(guild_id)]
        _save(data)
