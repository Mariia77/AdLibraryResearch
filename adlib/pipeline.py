# adlib/pipeline.py

from __future__ import annotations
import json
import re
import pandas as pd

from .io import OUTPUT_DIR, load_any, find_latest_input, try_json
from .enrich import (
    to_dt,
    compute_duration_hours,
    infer_media_mix,
    detect_lang,
    has_us,
    proxy_score_row,
)

RENAME_MAP = {
    "ad_archive_id": "id", "ad_id": "id", "id": "id",
    "page_name": "page_name", "pageName": "page_name",
    "ad_creative_body": "ad_creative_body",        # канон
    "ad_creative_bodies": "ad_creative_bodies",    # источник для коалесса
    "adText": "ad_creative_body",
    "ad_snapshot_url": "ad_snapshot_url", "adSnapshotURL": "ad_snapshot_url",
    "ad_delivery_start_time": "ad_delivery_start_time", "startDate": "ad_delivery_start_time",
    "ad_delivery_stop_time": "ad_delivery_stop_time", "endDate": "ad_delivery_stop_time",
    "ad_reached_countries": "ad_reached_countries", "countries": "ad_reached_countries",
    "delivery_by_region": "delivery_by_region",
    "ad_creative_link_url": "ad_creative_link_url",
    "impressions": "impressions", "spend": "spend", "currency": "currency",
    "languages": "languages", "language": "languages",
    "publisher_platforms": "publisher_platforms",
    "demographic_distribution": "demographic_distribution",
}

NEED_COLS = [
    "id", "page_name",
    "ad_creative_body",
    "ad_creative_bodies",
    "ad_snapshot_url",
    "ad_delivery_start_time", "ad_delivery_stop_time",
    "ad_reached_countries",
    "ad_creative_link_url", "impressions", "spend", "currency",
    "languages", "publisher_platforms", "delivery_by_region", "demographic_distribution",
]

def _norm_lang_code(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None

    if isinstance(val, list):
        for x in val:
            if isinstance(x, str) and x.strip():
                return _norm_lang_code(x)
        return None

    s = str(val).strip()
    if not s:
        return None

    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            return _norm_lang_code(arr)
        except Exception:
            pass

    s = s.strip("\"'").lower()
    if "," in s:
        s = s.split(",")[0].strip()
    s = s.split("_")[0].split("-")[0]

    if re.fullmatch(r"[a-z]{2}", s):
        return s
    if "english" in s:
        return "en"
    return None


def _coalesce_creative_body(row: pd.Series) -> str:
    body = row.get("ad_creative_body")
    if isinstance(body, str) and body.strip():
        return body.strip()

    bodies = row.get("ad_creative_bodies")
    if isinstance(bodies, list):
        for x in bodies:
            if isinstance(x, str) and x.strip():
                return x.strip()
    elif isinstance(bodies, str) and bodies.strip():
        s = bodies.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    for x in arr:
                        if isinstance(x, str) and x.strip():
                            return x.strip()
            except Exception:
                pass
        return s
    return ""


def main():
    src = find_latest_input()
    print(f"[i] Input file: {src.name}")
    raw = load_any(src)

    for c in list(raw.columns):
        if c in RENAME_MAP:
            raw.rename(columns={c: RENAME_MAP[c]}, inplace=True)

    for col in NEED_COLS:
        if col not in raw.columns:
            raw[col] = pd.NA

    for col in ["delivery_by_region", "demographic_distribution", "languages",
                "publisher_platforms", "impressions", "spend", "ad_creative_bodies"]:
        if col in raw.columns:
            raw[col] = raw[col].apply(try_json)

    raw["ad_creative_body"] = raw.apply(_coalesce_creative_body, axis=1)

    base = raw[NEED_COLS].copy()
    base.drop_duplicates(subset=["id"], inplace=True)

    base["ad_delivery_start_time_dt"] = base["ad_delivery_start_time"].apply(to_dt)
    base["ad_delivery_stop_time_dt"]  = base["ad_delivery_stop_time"].apply(to_dt)

    base["duration_hours"] = base.apply(
        lambda r: compute_duration_hours(
            r["ad_delivery_start_time_dt"],
            r["ad_delivery_stop_time_dt"],
            r["ad_creative_body"]
        ),
        axis=1
    )
    base["media_mix"] = raw.apply(infer_media_mix, axis=1)

    lang_from_col = base["languages"].apply(_norm_lang_code)
    fallback_lang = base["ad_creative_body"].apply(
        lambda t: detect_lang(t) if isinstance(t, str) and t.strip() else None
    )
    base["language"] = lang_from_col.where(lang_from_col.notna(), fallback_lang)
    base["language"] = base["language"].fillna("unknown").astype(str).str.lower()

    base["is_usa"] = base["ad_reached_countries"].apply(has_us) | base["delivery_by_region"].apply(has_us)
    base["proxy_score"] = base.apply(proxy_score_row, axis=1)

    canon_path = OUTPUT_DIR / "ad_library_canonical_output.csv"
    top10_path = OUTPUT_DIR / "top10_usa_ads.csv"
    base.to_csv(canon_path, index=False)
    base[base["is_usa"]].sort_values("proxy_score", ascending=False).head(10).to_csv(top10_path, index=False)

    media_counts = base["media_mix"].value_counts().to_dict()
    lang_counts = (
        base["language"].fillna("unknown").astype(str).str.lower()
        .value_counts()
        .drop(labels=["unknown"], errors="ignore")
        .head(5)
        .to_dict()
    )
    stats = {"total_ads": int(len(base)), "usa_ads": int(base["is_usa"].sum())}
    report = OUTPUT_DIR / "README_RUN.md"
    report.write_text(
f"""# Ad Library — Offline Run Report

**Source file**: `{src.name}`

## 📊 Statistics
- Total ads: **{stats['total_ads']}**
- USA ads: **{stats['usa_ads']}**

## 🖼️ Media mix
{media_counts}

> **Note on media mix:** the CSV export from Facebook Ad Library does **not** include explicit information about creative types (image/video).  
> The current implementation of `media_mix` relies on offline heuristics (checking URL/text hints).  
> If there are no such hints in the raw data, you will see `none`.  
> Once media-related hints appear (e.g., `.mp4`, `.jpg`, words like `video`/`image`), the classification will automatically work and output `image-only` / `video-only` / `both`.

## 🌐 Top languages
{lang_counts}

## 🏆 Ranking
Each ad receives a **proxy performance score** (0..1), then we filter `is_usa == True` and select the **Top-10** by descending score.

**Formula:**
proxy_score =
  0.5 * f(duration_hours)         # longer run = more budget/stability
+ 0.3 * f(len(ad_creative_body))  # meaningful text creative
+ 0.15 (if USA targeting)         # requirement of the task
+ media_bonus                     # video > image > none
    
where media_bonus: both=0.10, video-only=0.07, image-only=0.05, none=0.0
f(·) = smoothing (tanh) to clip extreme values

**Why these ads are top performers:**
- Longer runtime (`duration_hours`) ⇒ indicates sustained budget and delivery  
- Richer ad copy ⇒ better clarity and relevance  
- USA targeting ⇒ matches task requirements  
- Media presence (video/both) ⇒ typically drives higher engagement  
    
## 📦 Output files
- `{canon_path.name}` — canonical dataset with enrichment features  
- `{top10_path.name}` — Top-10 USA ads by `proxy_score`
    
""",
    encoding="utf-8",
)

