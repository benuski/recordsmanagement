# Ohio General Schedules Fix

## Problem

The Ohio harvester was **only downloading specific agency schedules** (`/Schedule/Details/XXXXX`) but **missing the general schedules** that apply to all Ohio agencies.

Looking at your archive scripts, the old workflow correctly handled both:
- **General schedules**: Downloaded from `/Schedule` (with variants for Repository and Specific functions)
- **Specific schedules**: Downloaded from `/Schedule/Details/{ID}`

## Solution

### 1. Added General Schedule Downloader

Created new function `download_general_schedule()` in [processing/oh/harvester.py](processing/oh/harvester.py:38-76) that downloads 3 general schedule pages:

```python
general_urls = [
    (f"{base_url}/Schedule", "gen_1.html"),
    (f"{base_url}/Schedule?Function=Repository", "gen_repository.html"),
    (f"{base_url}/Schedule?Function=Specific", "gen_specific.html"),
]
```

### 2. Added `spec_` Prefix to Specific Schedules

Modified `download_detail_pages()` to save specific agency schedules with a `spec_` prefix:
- **Before**: `34.html`
- **After**: `spec_34.html`

This makes it easy to distinguish between general and specific schedules.

### 3. Updated Harvest Pipeline

Modified [harvest.py](harvest.py:133-139) to:
1. Download general schedules first
2. Then download specific agency schedules
3. Fixed pruning logic to account for `spec_` prefix

### 4. Benefits

✅ **Complete coverage**: Now captures all Ohio retention schedules (general + specific)
✅ **Clear naming**: `gen_*.html` vs `spec_*.html` makes the type obvious
✅ **Incremental updates**: Uses `If-Modified-Since` for both general and specific schedules
✅ **Matches archive approach**: Aligns with the working logic from your old scripts

## Files Modified

- ✅ [processing/oh/harvester.py](processing/oh/harvester.py)
  - Added `download_general_schedule()` function (lines 38-76)
  - Changed specific schedule filenames to use `spec_` prefix (line 85)
- ✅ [harvest.py](harvest.py)
  - Added call to `download_general_schedule()` (lines 133-136)
  - Fixed pruning logic for `spec_*.html` files (line 116)

## What Happens When You Run It

```bash
~/.pixi/bin/pixi run python harvest.py \
  --input-directory processing/oh/ohio_specific \
  --output-directory data/oh \
  --state-code oh
```

**Download phase:**
1. Harvests all specific schedule URLs from search results
2. Downloads 3 general schedule pages → `gen_1.html`, `gen_repository.html`, `gen_specific.html`
3. Downloads ~10,500 specific schedules → `spec_34.html`, `spec_1002.html`, etc.

**Parse phase:**
1. Parses `gen_*.html` files → groups all general records in `general.json`
2. Parses `spec_*.html` files → groups by `schedule_id` prefix (e.g., `965.json`, `054.json`)

## Current Status

You already have 149 general schedules in `data/oh/general.json` from a previous run. When you re-run the pipeline, it will:
- Download the latest general schedules (if modified)
- Parse them alongside the specific schedules
- Group everything correctly

## Next Steps

1. Re-run the Ohio extraction to verify it downloads all 3 general schedule pages
2. Check that both general and specific schedules are parsed correctly
3. Optionally delete the test files and documentation

---

**Note**: The general schedules are table-based HTML (4 columns: Series ID, Title, Description, Retention), while specific schedules are detail pages with labeled fields. The existing parsers already handle both formats correctly.
