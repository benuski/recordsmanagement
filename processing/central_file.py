import json
import re
import logging
from pathlib import Path
from datetime import date
from collections import defaultdict
from word2number import w2n
from copy import deepcopy
from processing.base_config import StateScheduleConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema Mapping (Flat to Nested)
# ---------------------------------------------------------------------------
SCHEMA_MAP = {
    'state': ('schedule_metadata', 'state'),
    'agency_name': ('schedule_metadata', 'agency_name'),
    'schedule_type': ('schedule_metadata', 'schedule_type'),
    'schedule_id': ('schedule_metadata', 'schedule_id'),
    'url': ('schedule_metadata', 'url'),
    
    'series_id': ('series_metadata', 'series_id'),
    'series_title': ('series_metadata', 'series_title'),
    'series_description': ('series_metadata', 'series_description'),
    'legal_citation': ('series_metadata', 'legal_citation'),
    
    'trigger_event': ('retention_rules', 'trigger_event'),
    'retention_years': ('retention_rules', 'duration_years'),
    'retention_months': ('retention_rules', 'duration_months'),
    'disposition': ('retention_rules', 'disposition'),
    'confidential': ('retention_rules', 'confidential_flag'),
    
    'last_checked': ('tracking_data', 'last_checked'),
    'last_updated': ('tracking_data', 'last_updated_by_state'),
    'next_update': ('tracking_data', 'next_update_due'),
    'comments': ('tracking_data', 'comments'),
    
    'rsin': ('state_specific_attributes', 'rsin')
}

def get_nested_val(record: dict, flat_key: str):
    """Helper to safely retrieve a value from nested dict using flat key mapping."""
    if flat_key not in SCHEMA_MAP:
        return record.get(flat_key)
    path = SCHEMA_MAP[flat_key]
    target = record
    for step in path:
        if isinstance(target, dict):
            target = target.get(step)
        else:
            return None
    return target

def set_nested_val(record: dict, flat_key: str, value):
    """Helper to safely set a value in nested dict using flat key mapping."""
    if flat_key not in SCHEMA_MAP:
        record[flat_key] = value
        return
    path = SCHEMA_MAP[flat_key]
    target = record
    for step in path[:-1]:
        if step not in target:
            target[step] = {}
        target = target[step]
    target[path[-1]] = value

# ---------------------------------------------------------------------------
# Record Life Cycle
# ---------------------------------------------------------------------------

def make_record(schema: dict, **overrides) -> dict:
    """Creates a nested record, mapping flat overrides to the new structure."""
    record = deepcopy(schema) if schema else {}
    for key, value in overrides.items():
        set_nested_val(record, key, value)
    return record

def update_record(record: dict, **fields) -> dict:
    """Updates an existing nested record using flat keys."""
    for key, value in fields.items():
        set_nested_val(record, key, value)
    return record

def clean_record_fields(record: dict, config: StateScheduleConfig) -> dict:
    """Universal cleaning logic for nested records."""
    title = re.sub(r'\s+', ' ', str(get_nested_val(record, 'series_title') or '')).strip()
    desc = re.sub(r'\s+', ' ', str(get_nested_val(record, 'series_description') or '')).strip()
    
    # Raw components might be in root (retention_statement) or nested (disposition)
    raw_ret = str(record.pop('retention_statement', '')).strip()
    raw_disp = str(get_nested_val(record, 'disposition') or '').strip()

    # Combine into a single disposition statement
    full_disposition = raw_ret
    if raw_disp:
        if full_disposition:
            # Avoid duplicating if they are the same
            if raw_disp.lower() not in full_disposition.lower():
                full_disposition = f"{full_disposition}, {raw_disp}"
        else:
            full_disposition = raw_disp
    
    full_disposition = re.sub(r'\s+', ' ', full_disposition).strip()
    
    # Common typos
    full_disposition = re.sub(r'(?i)\bPermanen\b', 'Permanent', full_disposition)

    # 1. Extract Years/Months (kept for metadata richness)
    retention_years = get_nested_val(record, 'retention_years')
    retention_months = get_nested_val(record, 'retention_months')
    
    if retention_years is None:
        years_match = re.search(r'(\d+)\s*years?', full_disposition, re.IGNORECASE)
        if years_match:
            retention_years = int(years_match.group(1))
        
    if retention_months is None:
        months_match = re.search(r'(\d+)\s*months?', full_disposition, re.IGNORECASE)
        if months_match:
            retention_months = int(months_match.group(1))

    # 2. Confidential flag
    is_confidential = (
        "confidential" in full_disposition.lower()
        and "non-confidential" not in full_disposition.lower()
    )

    # 3. Infer Standardized Trigger Code
    trigger_code = get_nested_val(record, 'trigger_event')
    if not trigger_code:
        text_for_code = full_disposition.lower()
        if 'permanent' in text_for_code:
            trigger_code = "PM"
            retention_years = 999
        elif 'useful life' in text_for_code or 'administratively valuable' in text_for_code:
            trigger_code = "AV"
        elif 'superseded' in text_for_code:
            trigger_code = "US"
        elif 'fiscal year' in text_for_code:
            trigger_code = "FE"
        elif 'calendar year' in text_for_code:
            trigger_code = "CE"
        elif 'after' in text_for_code or 'completion' in text_for_code or 'closed' in text_for_code:
            trigger_code = "AC"
        elif retention_years is not None:
            trigger_code = "CR"
        else:
            trigger_code = "AV" # Default fallback

    # 4. Legal Citation extraction
    legal_citation = get_nested_val(record, 'legal_citation')
    if not legal_citation and config.legal_citation_pattern:
        cit_match = config.legal_citation_pattern.search(desc) or config.legal_citation_pattern.search(full_disposition)
        if cit_match:
            legal_citation = cit_match.group(0).strip()

    # Update nested structure
    set_nested_val(record, 'series_title', title)
    set_nested_val(record, 'series_description', desc)
    set_nested_val(record, 'legal_citation', legal_citation)
    set_nested_val(record, 'trigger_event', trigger_code)
    set_nested_val(record, 'retention_years', retention_years)
    set_nested_val(record, 'retention_months', retention_months)
    set_nested_val(record, 'disposition', full_disposition)
    set_nested_val(record, 'confidential', is_confidential)
    set_nested_val(record, 'last_checked', str(date.today()))

    return record

def score_records(records: list[dict], config: StateScheduleConfig) -> int:
    """Evaluates quality of nested records."""
    if not records:
        return -9999
    score = len(records) * 10
    seen_ids = set()
    for r in records:
        title = (get_nested_val(r, 'series_title') or '').strip()
        sid = (get_nested_val(r, 'series_id') or '')
        ret = (get_nested_val(r, 'disposition') or '').strip()
        if sid in seen_ids: score -= 20
        seen_ids.add(sid)
        if not title: score -= 15
        if not ret: score -= 10
        if len(title) > 200: score -= 50
    return score

def save_records(records: list[dict], output_dir: Path, group_by: str = None, default_filename: str = "output.json") -> None:
    """Saves records, supporting nested group_by keys."""
    if not records:
        return
    output_dir.mkdir(parents=True, exist_ok=True)
    if not group_by:
        output_path = output_dir / default_filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        return
    grouped = defaultdict(list)
    for rec in records:
        val = str(get_nested_val(rec, group_by) or 'general').strip()
        grouped[val].append(rec)
    for key, group_records in grouped.items():
        safe_key = "".join([c for c in key if c.isalnum() or c in (' ', '.', '-', '_')]).strip()
        output_path = output_dir / f"{safe_key}.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(group_records, f, indent=2, ensure_ascii=False)
