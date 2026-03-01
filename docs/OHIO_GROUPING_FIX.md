# Ohio Records Grouping Fix

## Problem

The previous implementation was incorrectly grouping Ohio records by the URL ID instead of by agency:

- **Wrong**: File `34.json` contained a single record from URL `.../Details/34`
- **Correct**: All records from the same agency should be grouped together (e.g., all OEB records in `OEB.json`)

## Root Cause

In the original code, the output filename was based on the HTML filename (URL ID):
```python
output_file = args.output_directory / f"{file_path.stem}.json"
```

This created ~10,500 individual JSON files, one per record, instead of grouping by agency.

## Solution

### 1. Cleaned Up Data Directory ✓
Removed all incorrectly split JSON files from `data/oh/`

### 2. Fixed Grouping Logic ✓
Updated [harvest.py:155-184](harvest.py:155-184) to group records by `agency_name`:

#### Key Changes:
```python
# Group records by agency_name
grouped_records = defaultdict(list)

for i, file_path in enumerate(html_files):
    if file_path.name.startswith("gen_"):
        records = process_ohio_general_html(file_path, output_schema)
        grouped_records['general'].extend(records)
    else:
        record = process_ohio_html(file_path, output_schema)
        if record:
            # Group by agency_name (e.g., "OEB", "LCC", "DAS")
            agency_name = record.get('agency_name', '').strip()
            if agency_name:
                grouped_records[agency_name].append(record)

# Write grouped records to files
for agency_name, records in grouped_records.items():
    output_file = args.output_directory / f"{agency_name}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
```

## Examples

### Before (Wrong)
```
data/oh/34.json       → [{"agency_name": "OEB", ...}]  (1 record)
data/oh/1002.json     → [{"agency_name": "LCC", ...}]  (1 record)
data/oh/11406.json    → [{"agency_name": "OEB", ...}]  (1 record)
... 10,500+ individual files
```

### After (Correct)
```
data/oh/general.json  → [149 general schedules]
data/oh/OEB.json      → [all Ohio Educational Broadcasting records]
data/oh/LCC.json      → [all Liquor Control Commission records]
data/oh/DAS.json      → [all Administrative Services records]
... ~100-200 agency files total
```

**Benefits:**
- **Logical organization**: All records from the same agency are together
- **Easier navigation**: Users can find records by agency
- **Fewer files**: ~100-200 agency files instead of ~10,500 individual records
- **Better for version control**: Agency-level changes are tracked in one file

## Field Reference

Ohio records have these key identifiers:
- `agency_name`: Agency code (e.g., "OEB", "LCC", "DAS") - **used for grouping**
- `schedule_id`: Authorization Number (e.g., "374-0001", "50580524")
- `series_id`: Agency Series No. (e.g., "PI-001", "OTC501-143")

## Next Steps

Re-run the Ohio extraction to generate agency-grouped files:
```bash
~/.pixi/bin/pixi run python harvest.py \
  --input-directory processing/oh/ohio_specific \
  --output-directory data/oh \
  --state-code oh
```

Or skip download and just re-parse existing HTML:
```bash
~/.pixi/bin/pixi run python harvest.py \
  --input-directory processing/oh/ohio_specific \
  --output-directory data/oh \
  --state-code oh \
  --skip-dl
```

## Files Modified

- ✓ [harvest.py](../harvest.py) (lines 155-184)
- ✓ `data/oh/` (cleaned up all existing JSON files)
