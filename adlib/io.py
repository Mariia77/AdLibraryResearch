from __future__ import annotations
import json
from pathlib import Path
from typing import Any
import pandas as pd

INPUT_DIR = Path("input")
OUTPUT_DIR = Path("outputs")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

def load_any(p: Path) -> pd.DataFrame:
    if p.suffix.lower() == ".csv":
        return pd.read_csv(p, low_memory=False)
    if p.suffix.lower() == ".json":
        obj = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(obj, dict) and "data" in obj:
            obj = obj["data"]
        return pd.json_normalize(obj)
    if p.suffix.lower() == ".jsonl":
        rows = [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
        return pd.json_normalize(rows)
    raise ValueError(f"Unsupported file type: {p.name}")

def find_latest_input() -> Path:
    files = [*INPUT_DIR.glob("*.csv"), *INPUT_DIR.glob("*.json"), *INPUT_DIR.glob("*.jsonl")]
    if not files:
        raise FileNotFoundError("No files in input/ (.csv/.json/.jsonl). Put Ad Library export there.")
    return max(files, key=lambda p: p.stat().st_mtime)

def try_json(x: Any):
    if not isinstance(x, str):
        return x
    s = x.strip()
    if not s:
        return x
    try:
        return json.loads(s)
    except Exception:
        return x
