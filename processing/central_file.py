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
    # OLD FLAT KEY -> NEW NESTED PATH
    'state': ('schedule_metadata', 'state'),
    'agency_name': ('schedule_metadata', 'agency_name'),
    'schedule_type': ('schedule_metadata', 'schedule_type'),
    'schedule_id': ('schedule_metadata', 'schedule_id'),
    'url': ('schedule_metadata', 'url'),
    
    'series_id': ('series_metadata', 'series_id'),
    'series_title': ('series_metadata', 'series_title'),
    'series_description': ('series_metadata', 'series_description'),
    'legal_citation': ('series_metadata', 'legal_citation'),
    
    'retention_statement': ('retention_rules', 'trigger_event'),
    'retention_code': ('retention_rules', 'retention_code'),
    'retention_years': ('retention_rules', 'duration_years'),
    'retention_months': ('retention_rules', 'duration_months'),
    'disposition': ('retention_rules', 'disposition_method'),
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

# ---------------------------------------------------------------------------
# Retention & Trigger Standardization
# ---------------------------------------------------------------------------

TRIGGER_MAP = {
    # TX Code -> [list of patterns]
    'AC': [
        r'after\s+(?:file\s+)?(?:closed?|completion|expiration|settlement|separation|leaves?\s+office|inactive|termination|evaluation|event)',
        r'file\s+is\s+closed',
        r'completion\s+of',
        r'leaves?\s+office',
        r'agency\s+closure',
    ],
    'AV': [r'administratively\s+valuable', r'reference\s+value\s+ends', r'no\s+longer\s+needed'],
    'CE': [r'end\s+of\s+calendar\s+year', r'close\s+of\s+year', r'calendar\s+year'],
    'FE': [r'end\s+of\s+fiscal\s+year', r'fiscal\s+year'],
    'LA': [r'life\s+of\s+asset'],
    'PM': [r'permanent', r'permanently', r'archives'],
    'US': [r'superseded', r'obsolete', r'rescinded'],
}

def standardize_trigger(retention_text: str) -> str | None:
    """Maps raw retention text to a standardized TX-style code."""
    if not retention_text:
        return None
    
    text_lower = retention_text.lower()
    
    # Check for direct code matches (case-sensitive for codes)
    for code in TRIGGER_MAP.keys():
        if re.search(rf'\b{code}\b', retention_text):
            return code

    # Check for pattern matches
    for code, patterns in TRIGGER_MAP.items():
        for pattern in patterns:
            if re.search(pattern, text_lower):
                return code
                
    return None

def clean_record_fields(record: dict, config: StateScheduleConfig) -> dict:
    """Universal cleaning logic for nested records."""
    title = re.sub(r'\s+', ' ', str(get_nested_val(record, 'series_title') or '')).strip()
    desc = re.sub(r'\s+', ' ', str(get_nested_val(record, 'series_description') or '')).strip()
    retention = re.sub(r'\s+', ' ', str(get_nested_val(record, 'retention_statement') or '')).strip()
    disposition = re.sub(r'\s+', ' ', str(get_nested_val(record, 'disposition') or '')).strip()

    # Common typos
    retention = re.sub(r'(?i)\bPermanen\b', 'Permanent', retention)
    disposition = re.sub(r'(?i)\bPermanen\b', 'Permanent', disposition)

    # Simple disposition extraction
    disp_match = re.search(
        r'(?i)(Non-confidential Destruction|Confidential Destruction|Permanent, Archives|Permanent, In Agency|Archives|Destruction)$',
        disposition if disposition else retention
    )
    if disp_match and not disposition:
        disposition = disp_match.group(1).title()
        retention = retention[:disp_match.start()].strip()

    # Confidential flag
    is_confidential = (
        "confidential" in disposition.lower()
        and "non-confidential" not in disposition.lower()
    )

    # Years calculation
    retention_years = None
    retention_months = None
    
    # Try to extract a clean trigger by removing the duration part
    clean_trigger = retention
    
    # Check for '{N} years' or '{N} months'
    years_match = re.search(r'(\d+)\s*years?', retention, re.IGNORECASE)
    months_match = re.search(r'(\d+)\s*months?', retention, re.IGNORECASE)
    
    if years_match:
        retention_years = int(years_match.group(1))
        # Remove the 'N years' part from the trigger
        clean_trigger = re.sub(rf'\b{years_match.group(0)}\b', '', clean_trigger, flags=re.IGNORECASE)
        
    if months_match:
        retention_months = int(months_match.group(1))
        clean_trigger = re.sub(rf'\b{months_match.group(0)}\b', '', clean_trigger, flags=re.IGNORECASE)

    # Standardized Trigger Code
    retention_code = standardize_trigger(retention)

    # Clean up leftovers in trigger
    # Remove common boilerplate
    clean_trigger = re.sub(r'(?i)\b(Retain|then|destroy|transfer|to|the|State Records Center|on-site|in compliance with No\. \d+ on cover sheet|Total retention period)\b', '', clean_trigger)
    clean_trigger = re.sub(r'\b(plus|after|for|until)\b', '', clean_trigger, flags=re.IGNORECASE)
    
    # Remove everything after 'then' or 'plus' if they was meant to be the end
    clean_trigger = re.sub(r'(?i)\b(THEN|plus)\s+.*$', '', clean_trigger)

    clean_trigger = re.sub(r'\s+', ' ', clean_trigger).strip()
    clean_trigger = re.sub(r'^[,\.;\s:]+|[,\.;\s:]+$', '', clean_trigger) # Strip leading/trailing punctuation

    if 'permanent' in retention.lower() or 'permanent' in disposition.lower():
        retention_years = 999 
        # If it's permanent, the trigger is usually just 'Permanent'
        if not clean_trigger or clean_trigger.lower() == 'permanent':
            clean_trigger = "Permanent"
        retention_code = "PM"

    # Policy 10 Defaulting: If we have a duration but no trigger code was identified, 
    # and the trigger is basically empty or just generic "Retain", default to CE.
    if not retention_code and (retention_years is not None or retention_months is not None):
        # If it's a specific instruction like "after evaluation", it should have been caught by AC.
        # If we reach here, it's likely just "Retain 3 years".
        if not clean_trigger or clean_trigger.lower() in ['retain', '']:
            retention_code = 'CE'

    # Defaulting logic: If we have a code, use it. Otherwise use the clean string.
    trigger_event = retention_code if retention_code else (clean_trigger if clean_trigger else retention)

    # Update nested structure
    set_nested_val(record, 'series_title', title)
    set_nested_val(record, 'series_description', desc)
    set_nested_val(record, 'retention_statement', trigger_event)
    set_nested_val(record, 'retention_years', retention_years)
    set_nested_val(record, 'retention_months', retention_months)
    set_nested_val(record, 'disposition', disposition)
    set_nested_val(record, 'confidential', is_confidential)
    set_nested_val(record, 'last_checked', str(date.today()))
    set_nested_val(record, 'retention_code', retention_code or "")

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
        ret = (get_nested_val(r, 'retention_statement') or '').strip()

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
