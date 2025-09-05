# tracer/config.py
from pathlib import Path

# Where we persist small bot settings
SETTINGS_PATH = "data/settings.json"
Path("data").mkdir(parents=True, exist_ok=True)

# Map catalog: add/replace your real PNGs here
MAPS = {
    "livonia": {
        "name": "Livonia",
        "image": "assets/maps/livonia_base.PNG",
        "world_min_x": 0.0, "world_max_x": 12800.0,
        "world_min_z": 0.0, "world_max_z": 12800.0,
    },
    "chernarus": {
        "name": "Chernarus",
        "image": "assets/maps/chernarus_base.PNG",
        "world_min_x": 0.0, "world_max_x": 15360.0,
        "world_min_z": 0.0, "world_max_z": 15360.0,
    },
}

# Default settings used on first run (until /assignchannel is used)
DEFAULT_SETTINGS = {
    "bounty_channel_id": None,   # filled by /assignchannel
    "active_map": "livonia",     # "livonia" | "chernarus"
}
