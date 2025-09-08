# Ad Library — Offline Run Report

    **Source file**: `meta-ad-library-07.09.2025.csv`

    ## 📊 Statistics
    - Total ads: **71**
    - USA ads: **69**

    ## 🖼️ Media mix
    {'none': 71}

    > **Note on media mix:** the CSV export from Facebook Ad Library does **not** include explicit information about creative types (image/video).  
    > The current implementation of `media_mix` relies on offline heuristics (checking URL/text hints).  
    > If there are no such hints in the raw data, you will see `none`.  
    > Once media-related hints appear (e.g., `.mp4`, `.jpg`, words like `video`/`image`), the classification will automatically work and output `image-only` / `video-only` / `both`.

    ## 🌐 Top languages
    {'en': 71}

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
    - `ad_library_canonical_output.csv` — canonical dataset with enrichment features  
    - `top10_usa_ads.csv` — Top-10 USA ads by `proxy_score`
    
    