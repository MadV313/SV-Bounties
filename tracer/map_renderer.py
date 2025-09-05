# tracer/map_renderer.py
from PIL import Image, ImageDraw
import io
from tracer.config import MAPS
from utils.settings import load_settings

def _get_active_map_cfg(map_override: str | None = None):
    settings = load_settings()
    key = (map_override or settings.get("active_map") or "livonia").lower()
    cfg = MAPS.get(key)
    if not cfg:
        # fall back safely
        cfg = MAPS["livonia"]
        key = "livonia"
    return key, cfg

def world_to_px(x, z, cfg, width, height):
    u = (x - cfg["world_min_x"]) / (cfg["world_max_x"] - cfg["world_min_x"])
    v = 1.0 - (z - cfg["world_min_z"]) / (cfg["world_max_z"] - cfg["world_min_z"])
    return int(u * width), int(v * height)

def render_track_png(track_doc: dict, map_override: str | None = None, show_numbers: bool = True):
    _, cfg = _get_active_map_cfg(map_override)
    im = Image.open(cfg["image"]).convert("RGBA")
    draw = ImageDraw.Draw(im)
    W, H = im.size

    pts = track_doc.get("points", [])
    last_px = None
    for idx, p in enumerate(pts, start=1):
        px = world_to_px(p["x"], p["z"], cfg, W, H)
        if last_px:
            draw.line([last_px, px], width=3)
        r = 6
        draw.ellipse([px[0]-r, px[1]-r, px[0]+r, px[1]+r], width=3)
        if show_numbers:
            draw.text((px[0]+8, px[1]-10), str(idx))
        last_px = px

    # simple arrow on last point
    if last_px:
        draw.regular_polygon((last_px[0], last_px[1], 12), n_sides=3, rotation=90)

    buf = io.BytesIO()
    im.save(buf, format="PNG")
    buf.seek(0)
    return buf
