from __future__ import annotations
import re, json
from typing import Any
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from dateutil import parser as dtp
from langdetect import detect, DetectorFactory

DetectorFactory.seed = 0

US_STATES = {
    "ALABAMA","ALASKA","ARIZONA","ARKANSAS","CALIFORNIA","COLORADO","CONNECTICUT","DELAWARE","FLORIDA","GEORGIA",
    "HAWAII","IDAHO","ILLINOIS","INDIANA","IOWA","KANSAS","KENTUCKY","LOUISIANA","MAINE","MARYLAND","MASSACHUSETTS",
    "MICHIGAN","MINNESOTA","MISSISSIPPI","MISSOURI","MONTANA","NEBRASKA","NEVADA","NEW HAMPSHIRE","NEW JERSEY",
    "NEW MEXICO","NEW YORK","NORTH CAROLINA","NORTH DAKOTA","OHIO","OKLAHOMA","OREGON","PENNSYLVANIA","RHODE ISLAND",
    "SOUTH CAROLINA","SOUTH DAKOTA","TENNESSEE","TEXAS","UTAH","VERMONT","VIRGINIA","WASHINGTON","WEST VIRGINIA",
    "WISCONSIN","WYOMING","DISTRICT OF COLUMBIA","WASHINGTON DC","D.C.","DC","PUERTO RICO"
}

def to_dt(x: Any):
    if pd.isna(x) or x == "" or x is None:
        return None
    try:
        return dtp.parse(str(x))
    except Exception:
        return None

def parse_runtime_proxy(text: str) -> float:
    if not isinstance(text, str) or not text:
        return np.nan
    d = re.search(r"(\d+)\s*day", text, re.I)
    h = re.search(r"(\d+)\s*hr", text, re.I)
    m = re.search(r"(\d+)\s*min", text, re.I)
    days = int(d.group(1)) if d else 0
    hours = int(h.group(1)) if h else 0
    mins = int(m.group(1)) if m else 0
    total_h = days * 24 + hours + mins / 60.0
    return total_h if total_h > 0 else np.nan

def detect_lang(text: str) -> str:
    if not isinstance(text, str) or not text.strip():
        return "unknown"
    try:
        return detect(text)
    except Exception:
        return "unknown"

def infer_media_mix(row: pd.Series) -> str:
    has_video = False
    has_image = False
    for col in row.index:
        lc = col.lower()
        if ("video" in lc and pd.notna(row[col])) or ("mp4" in str(row.get(col))):
            has_video = True
        if any(k in lc for k in ["image", "thumbnail", "picture"]) and pd.notna(row[col]):
            has_image = True
    snap = " ".join(str(row.get(c, "")) for c in ["ad_snapshot_url","snapshot_url","ad_creative_link_url"])
    s = snap.lower()
    if any(k in s for k in ["video",".mp4"]): has_video = True
    if any(k in s for k in ["image",".jpg",".jpeg",".png",".webp"]): has_image = True
    if has_video and has_image: return "both"
    if has_video: return "video-only"
    if has_image: return "image-only"
    return "none"

def has_us(countries_or_regions):
    if isinstance(countries_or_regions, list):
        for el in countries_or_regions:
            if isinstance(el, dict):
                reg = str(el.get("region","")).upper()
                if reg == "US" or reg in US_STATES:
                    return True
            elif isinstance(el, str):
                if el.strip().upper() in US_STATES or el.strip().upper() == "US":
                    return True
        return False
    if isinstance(countries_or_regions, str):
        s = countries_or_regions.upper()
        return ("US" in s) or any(state in s for state in US_STATES)
    return False

def compute_duration_hours(start_dt, stop_dt, fallback_text) -> float:
    now = datetime.now(timezone.utc)
    if start_dt:
        stop = stop_dt if stop_dt else now
        return max(0.0, (stop - start_dt).total_seconds()/3600.0)
    return parse_runtime_proxy(str(fallback_text))

def proxy_score_row(r: pd.Series) -> float:
    dur = float(r["duration_hours"]) if pd.notna(r["duration_hours"]) else 0.0
    dur_norm = np.tanh(dur/72.0)             # ~3 days saturation
    txt_len  = len(str(r["ad_creative_body"])) if pd.notna(r["ad_creative_body"]) else 0
    txt_norm = np.tanh(txt_len/400.0)
    usa_bonus = 0.15 if r["is_usa"] else 0.0
    media_bonus = {"both":0.10,"video-only":0.07,"image-only":0.05,"none":0.0}.get(r["media_mix"],0.0)
    return round(0.5*dur_norm + 0.3*txt_norm + usa_bonus + media_bonus, 4)
