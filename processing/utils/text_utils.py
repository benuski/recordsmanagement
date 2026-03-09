import re
from datetime import date
from word2number import w2n
from processing.base_config import StateScheduleConfig

def split_title_and_description(raw_text: str) -> tuple[str, str]:
    match = re.search(
        r'((?:This series\s+)?(?:documents|Collects|Verifies|Consists|consists)\b.*)',
        raw_text, re.IGNORECASE
    )
    if match:
        return raw_text[:match.start()].strip(), match.group(1).strip()
    parts = raw_text.split('.', 1)
    if len(parts) > 1 and len(parts[0]) < 100:
        return parts[0].strip(), parts[1].strip()
    return raw_text.strip(), ""

def clean_record_fields(record: dict, config: StateScheduleConfig) -> dict:
    title = re.sub(r'\s+', ' ', record.get('series_title', '')).strip()
    desc = re.sub(r'\s+', ' ', record.get('series_description', '')).strip()
    retention = re.sub(r'\s+', ' ', record.get('retention_statement', '')).strip()
    disposition = re.sub(r'\s+', ' ', record.get('disposition', '')).strip()

    # Fix common typos from source data
    retention = re.sub(r'(?i)\bPermanen\b', 'Permanent', retention)
    disposition = re.sub(r'(?i)\bPermanen\b', 'Permanent', disposition)

    disp_match = re.search(
        r'(?i)(Non-confidential Destruction|Confidential Destruction|Permanent, Archives|Permanent, In Agency|Archives|Destruction)$',
        disposition if disposition else retention
    )
    if disp_match and not disposition:
        disposition = disp_match.group(1).title()
        retention = retention[:disp_match.start()].strip()

    conf_match = re.search(r'(?i)\b(Non-confidential|Confidential)\b', retention)
    if conf_match:
        retention = retention[:conf_match.start()] + retention[conf_match.end():]
        retention = re.sub(r'\s+', ' ', retention).strip()
        if not disposition.lower().startswith(conf_match.group(1).lower()):
            disposition = f"{conf_match.group(1).title()} {disposition}".strip()

    for kw in ["Destruction", "Archives"]:
        kw_match = re.search(fr'(?i)\b{kw}\b', retention)
        if kw_match and kw.lower() not in disposition.lower():
            retention = retention[:kw_match.start()] + retention[kw_match.end():]
            retention = re.sub(r'\s+', ' ', retention).strip()
            disposition = f"{disposition} {kw}".strip()

    legal_citation = record.get('legal_citation', '').strip()
    citation_match = config.legal_citation_pattern.search(desc)
    if citation_match:
        found_citation = citation_match.group(1).strip()
        if not legal_citation:
            legal_citation = found_citation
        elif found_citation not in legal_citation:
            legal_citation = f"{legal_citation}; {found_citation}"
        
        desc = desc[:citation_match.start()].strip()
        desc = re.sub(r'[\.,;:]$', '', desc).strip()

    # Universal Retention Years Calculation
    retention_years_match = re.search(r'\(?(\d+)\)?\s*year', retention, re.IGNORECASE)
    word_match = re.search(r'\b([a-zA-Z]+(?:-[a-zA-Z]+)?)\b\s*year', retention, re.IGNORECASE)

    if retention_years_match:
        retention_years = int(retention_years_match.group(1))
    elif word_match:
        try:
            retention_years = w2n.word_to_num(word_match.group(1).lower())
        except ValueError:
            retention_years = None
    elif 'permanent' in retention.lower() or 'permanent' in disposition.lower():
        retention_years = None
    else:
        retention_years = None

    is_confidential = (
        "confidential" in disposition.lower()
        and "non-confidential" not in disposition.lower()
    )

    record.update({
        'series_title': title,
        'series_description': desc,
        'retention_statement': retention,
        'retention_years': retention_years,
        'disposition': disposition,
        'confidential': is_confidential,
        'legal_citation': legal_citation,
        'last_checked': str(date.today())
    })
    return record
