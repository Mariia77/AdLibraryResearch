from __future__ import annotations
import pandas as pd
from pathlib import Path
from .io import INPUT_DIR, OUTPUT_DIR, load_any, find_latest_input, try_json
from .enrich import to_dt, compute_duration_hours, infer_media_mix, detect_lang, has_us, proxy_score_row

RENAME_MAP = {
    "ad_archive_id":"id", "ad_id":"id", "id":"id",
    "page_name":"page_name","pageName":"page_name",
    "ad_creative_body":"ad_creative_body","adText":"ad_creative_body",
    "ad_snapshot_url":"ad_snapshot_url","adSnapshotURL":"ad_snapshot_url",
    "ad_delivery_start_time":"ad_delivery_start_time","startDate":"ad_delivery_start_time",
    "ad_delivery_stop_time":"ad_delivery_stop_time","endDate":"ad_delivery_stop_time",
    "ad_reached_countries":"ad_reached_countries","countries":"ad_reached_countries",
    "ad_creative_link_url":"ad_creative_link_url",
    "impressions":"impressions","spend":"spend","currency":"currency",
    "languages":"languages","publisher_platforms":"publisher_platforms",
    "delivery_by_region":"delivery_by_region",
    "demographic_distribution":"demographic_distribution",
}

NEED_COLS = [
    "id","page_name","ad_creative_body","ad_snapshot_url",
    "ad_delivery_start_time","ad_delivery_stop_time",
    "ad_reached_countries",
    "ad_creative_link_url","impressions","spend","currency",
    "languages","publisher_platforms","delivery_by_region","demographic_distribution",
]

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

    for col in ["delivery_by_region","demographic_distribution","languages","publisher_platforms","impressions","spend"]:
        if col in raw.columns:
            raw[col] = raw[col].apply(try_json)

    base = raw[NEED_COLS].copy()
    base.drop_duplicates(subset=["id"], inplace=True)

    base["ad_delivery_start_time_dt"] = base["ad_delivery_start_time"].apply(to_dt)
    base["ad_delivery_stop_time_dt"]  = base["ad_delivery_stop_time"].apply(to_dt)

    base["duration_hours"] = base.apply(
        lambda r: compute_duration_hours(r["ad_delivery_start_time_dt"], r["ad_delivery_stop_time_dt"], r["ad_creative_body"]),
        axis=1
    )
    base["media_mix"] = raw.apply(infer_media_mix, axis=1)
    base["language"]  = base["ad_creative_body"].apply(lambda x: detect_lang(x) if isinstance(x,str) else "unknown")
    base["is_usa"]    = base["ad_reached_countries"].apply(has_us) | base["delivery_by_region"].apply(has_us)
    base["proxy_score"] = base.apply(proxy_score_row, axis=1)

    canon_path = OUTPUT_DIR / "ad_library_canonical_output.csv"
    top10_path = OUTPUT_DIR / "top10_usa_ads.csv"
    base.to_csv(canon_path, index=False)
    base[base["is_usa"]].sort_values("proxy_score", ascending=False).head(10).to_csv(top10_path, index=False)

    # report
    report = OUTPUT_DIR / "README_RUN.md"
    media_counts = base["media_mix"].value_counts().to_dict()
    lang_top = base["language"].value_counts().head(5).to_dict()
    stats = {"total_ads": int(len(base)), "usa_ads": int(base["is_usa"].sum())}
    report.write_text(
        f"""# Ad Library — Offline Run Report

**Source file**: `{src.name}`

## Stats
- Total ads: **{stats['total_ads']}**
- USA ads: **{stats['usa_ads']}**

## Media mix
{media_counts}

## Top languages (5)
{lang_top}

## Output files
- `{canon_path.name}` — canonical dataset
- `{top10_path.name}` — Top-10 USA by proxy_score
""",
        encoding="utf-8",
    )
    print(f"[✓] Done:\n  - {canon_path}\n  - {top10_path}\n  - {report}")
