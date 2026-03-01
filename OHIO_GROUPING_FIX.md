# Ohio Records Grouping Fix

## Problem

The previous implementation was incorrectly grouping Ohio records by the URL ID instead of the `schedule_id` prefix:

- **Wrong**: File `34.json` contained records from URL `.../Details/34`
- **Correct**: Records with `schedule_id: "965-0012"` should be grouped in `965.json`

## Root Cause

In [harvest.py:159](harvest.py:159), the code was using:
```python
output_file = args.output_directory / f"{file_path.stem}.json"
```

This used the HTML filename (`spec_34.html` → `spec_34.json`) instead of extracting the `schedule_id` prefix.

## Solution

### 1. Cleaned Up Data Directory ✓
Deleted **10,452 incorrectly named JSON files** from `data/oh/`:
- Removed: `34.json`, `1002.json`, `1003.json`, etc.
- Preserved: `general.json`, `repository.json`, `specific.json`

### 2. Fixed Grouping Logic ✓
Updated [harvest.py:139-179](harvest.py:139-179) to:

1. **Parse all records first** into memory
2. **Extract schedule_id prefix** (digits before the dash)
3. **Group records** by this prefix using `defaultdict`
4. **Write one file per schedule prefix**

#### Key Changes:
```python
# Group records by schedule_id prefix (digits before dash)
grouped_records = defaultdict(list)

for i, file_path in enumerate(html_files):
    if file_path.name.startswith("gen_"):
        records = process_ohio_general_html(file_path, output_schema)
        grouped_records['general'].extend(records)
    else:
        record = process_ohio_html(file_path, output_schema)
        if record:
            schedule_id = record.get('schedule_id', '')
            if schedule_id and '-' in schedule_id:
                prefix = schedule_id.split('-')[0]  # Extract "965" from "965-0012"
                grouped_records[prefix].append(record)

# Write grouped records to files
for schedule_prefix, records in grouped_records.items():
    output_file = args.output_directory / f"{schedule_prefix}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
```

## Examples

### Before (Wrong)
```
data/oh/34.json       → [{"schedule_id": "965-0012", ...}]
data/oh/35.json       → [{"schedule_id": "965-0015", ...}]
data/oh/1002.json     → [{"schedule_id": "054-0091", ...}]
```

### After (Correct)
```
data/oh/965.json      → [
                          {"schedule_id": "965-0012", ...},
                          {"schedule_id": "965-0015", ...}
                        ]
data/oh/054.json      → [{"schedule_id": "054-0091", ...}]
```

## Validation

Created `test_ohio_grouping.py` to verify the logic:
```bash
~/.pixi/bin/pixi run python test_ohio_grouping.py
```

**Results:**
✓ All test cases pass
✓ Records correctly grouped by schedule_id prefix
✓ General schedules handled separately

## Next Steps

1. Re-run the Ohio extraction:
   ```bash
   ~/.pixi/bin/pixi run python harvest.py \
     --input-directory <html_dir> \
     --output-directory data/oh \
     --state-code oh
   ```

2. Verify the output files are grouped correctly by schedule_id prefix

3. Delete test files if desired:
   - `test_ohio_grouping.py`
   - `OHIO_GROUPING_FIX.md`

## Files Modified

- ✓ `harvest.py` (lines 139-179)
- ✓ `data/oh/` (cleaned up ~10,452 incorrect files)

## Files Created

- ✓ `test_ohio_grouping.py` (test script)
- ✓ `OHIO_GROUPING_FIX.md` (this document)
