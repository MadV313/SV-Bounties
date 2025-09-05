# tracer/config.py
from pathlib import Path

# Where we persist small bot settings
SETTINGS_PATH = "data/settings.json"
Path("data").mkdir(parents=True, exist_ok=True)

# Local and external link storage
LOCAL_LINKS_PATH = "data/linked_players.json"
EXTERNAL_LINKS_PATH = None  # optional: default external repo path, override with /setexternallinks
INDEX_PATH = "data/players_index.json"
TRACKS_DIR = "data/player_tracks"
MAX_POINTS_PER_PLAYER = 5000

# Map catalog
MAPS = {
    "livonia": {
        "name": "Livonia",
        "image": "assets/maps/livonia_base.PNG",  # watch case!
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

# tracer/config.py (append to DEFAULT_SETTINGS)
DEFAULT_SETTINGS = {
    "bounty_channel_id": None,   # public bounties channel
    "admin_channel_id": None,    # private admin ops (trace, internals)
    "active_map": "livonia",
}
