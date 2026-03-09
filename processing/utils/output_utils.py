import json
import logging
from pathlib import Path
from collections import defaultdict

logger = logging.getLogger(__name__)

def save_records(records: list[dict], output_dir: Path, group_by: str = None, default_filename: str = "output.json") -> None:
    """
    Saves a list of records to JSON file(s).
    
    Args:
        records: List of record dictionaries.
        output_dir: Directory to save the files.
        group_by: Optional key to group records by (e.g., 'agency_name' or 'schedule_id').
                 If provided, multiple files will be created named {group_value}.json.
        default_filename: Filename to use if not grouping or if group key is missing.
    """
    if not records:
        logger.warning(f"No records to save to {output_dir}")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    if not group_by:
        output_path = output_dir / default_filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(records)} records to {output_path}")
        return

    # Grouping logic
    grouped = defaultdict(list)
    for rec in records:
        val = rec.get(group_by, "").strip()
        key = val if val else "general"
        grouped[key].append(rec)

    for key, group_records in grouped.items():
        # Sanitize key for filename
        safe_key = "".join([c for c in key if c.isalnum() or c in (' ', '.', '-', '_')]).strip()
        output_path = output_dir / f"{safe_key}.json"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(group_records, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(group_records)} records to {output_path}")
