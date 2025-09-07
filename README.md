# 💡Ad-Library Data Pipeline (offline / file-only)

The pipeline processes **Facebook Ad Library** exports **without tokens or API**.  
It performs:

- normalization of raw columns into a canonical schema,
- enrichment: `duration_hours`, `media_mix`, `language`, `is_usa`,
- computes ranking metric: `proxy_score`,
- selects the **Top-10 ads for USA**,
- generates a mini-report.

## ⛴ Docker run

```bash
docker run --rm \
  -v "$(pwd)/input:/app/input" \
  -v "$(pwd)/outputs:/app/outputs" \
  adlib-file-pipeline:latest
```

## 📂 Project Structure

```markdown
ad-library-pipeline/
├─ adlib/
│  ├─ __init__.py
│  ├─ io.py              # load input files, parse JSON strings
│  ├─ enrich.py          # enrichment functions: dates, duration, language, media_mix, is_usa, score
│  └─ pipeline.py        # # orchestration: read -> normalize -> enrich -> save
├─ run_file_only.py      # thin wrapper: calls pipeline.main()
├─ requirements.txt
├─ Dockerfile
├─ .dockerignore
├─ README.md
├─ input/               
│  └─ .gitkeep
└─ outputs/              # pipeline results
   └─ .gitkeep
```
