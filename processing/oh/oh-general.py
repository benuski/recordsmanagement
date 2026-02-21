import json
import logging
import re
from datetime import date
from pathlib import Path
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
log = logging.getLogger(__name__)

# --- Module-level constants ---

WORD_TO_NUM = {
    'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
    'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
    'eleven': 11, 'twelve': 12, 'fifteen': 15, 'sixteen': 16
}

_WHITESPACE_RE      = re.compile(r'\s+')
_PERMANENT_RE       = re.compile(r'permanently?', re.IGNORECASE)
_RETAIN_EMPTY_RE    = re.compile(r'^Retain[\s\.]*$', re.IGNORECASE)
_THEN_DISP_RE       = re.compile(r'(?:,\s*)?then\s+(.*)', re.IGNORECASE)
_OAKS_RE            = re.compile(r'\.?\s*OAKS:.*', re.IGNORECASE)
_TRAILING_PUNCT_RE  = re.compile(r'[\.,;:]$')
_DIGIT_YEAR_RE      = re.compile(r'(\d+)\s*year', re.IGNORECASE)
_WORD_NUM_RE        = re.compile(
    r'\b(' + '|'.join(WORD_TO_NUM.keys()) + r')\b\s*year', re.IGNORECASE
)
_LEGAL_CITATION_RE  = re.compile(
    r'(\bORC\s*\d+\.\d+|\b\d+\s*CFR\s*\d+|\b\d+\s*USC\s*\d+)', re.IGNORECASE
)


# --- Helpers ---

def _normalize(s: str) -> str:
    """Collapse internal whitespace and strip."""
    return _WHITESPACE_RE.sub(' ', s).strip()

def _cap_first(s: str) -> str:
    """Capitalize only the first character, leaving the rest unchanged."""
    return s[0].upper() + s[1:] if s else ""

def _clean_punct(s: str) -> str:
    return _TRAILING_PUNCT_RE.sub('', s).strip()


# --- Core logic ---

def clean_record_fields(record: dict) -> dict:
    title       = _normalize(record.get('series_title', ''))
    desc        = _normalize(record.get('series_description', ''))
    retention   = _normalize(record.get('retention_statement', ''))
    disposition = _normalize(record.get('disposition', ''))

    # Catch explicit "Retain permanently" statements first
    if not disposition and _PERMANENT_RE.search(retention):
        disposition = "Permanent"
        retention = _PERMANENT_RE.sub('', retention).strip()
        retention = _RETAIN_EMPTY_RE.sub('', retention).strip()

    # Ohio bundles disposition into the retention string via "then ..."
    if not disposition:
        disp_match = _THEN_DISP_RE.search(retention)
        if disp_match:
            extracted_disp = disp_match.group(1).strip()

            oaks_match = _OAKS_RE.search(extracted_disp)
            if oaks_match:
                oaks_text = oaks_match.group(0)
                extracted_disp = extracted_disp.replace(oaks_text, '').strip()
                retention = retention.replace(disp_match.group(0), oaks_text).strip()
            else:
                retention = retention.replace(disp_match.group(0), '').strip()

            extracted_disp = _clean_punct(extracted_disp)
            retention = _clean_punct(retention)

            disp_lower = extracted_disp.lower()
            if 'archives' in disp_lower and not ('possible' in disp_lower or 'review' in disp_lower):
                disposition = "Permanent"
            else:
                disposition = _cap_first(extracted_disp)

    # Extract legal citation — check description first, then retention
    m = _LEGAL_CITATION_RE.search(desc) or _LEGAL_CITATION_RE.search(retention)
    legal_citation = m.group(1).strip() if m else ""

    # Determine numeric retention years
    if 'permanent' in retention.lower() or 'permanent' in disposition.lower():
        retention_years = None
    else:
        digit_m = _DIGIT_YEAR_RE.search(retention)
        if digit_m:
            retention_years = int(digit_m.group(1))
        else:
            word_m = _WORD_NUM_RE.search(retention)
            retention_years = WORD_TO_NUM[word_m.group(1).lower()] if word_m else None

    disp_lower = disposition.lower()
    is_confidential = 'confidential' in disp_lower and 'non-confidential' not in disp_lower

    record.update({
        'series_title':        title,
        'series_description':  desc,
        'retention_statement': retention,
        'retention_years':     retention_years,
        'disposition':         disposition,
        'confidential':        is_confidential,
        'legal_citation':      legal_citation,
    })
    return record


def extract_to_json(html_file_path: Path, output_json_path: Path) -> None:
    with open(html_file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')

    table_body = soup.find('tbody')
    if not table_body:
        log.error("Could not find a <tbody> in %s", html_file_path)
        return

    schedules = []
    for row in table_body.find_all('tr'):
        cols = row.find_all('td')

        if len(cols) != 4:
            log.warning("Skipping row with %d column(s): %r", len(cols), row.get_text()[:80])
            continue

        raw_record = {
            "state":               "oh",
            "agency_name":         "",
            "schedule_type":       "general",
            "schedule_id":         "General",
            "series_id":           cols[0].get_text(separator=' ', strip=True),
            "series_title":        cols[1].get_text(separator=' ', strip=True),
            "series_description":  cols[2].get_text(separator=' ', strip=True),
            "retention_statement": cols[3].get_text(separator=' ', strip=True),
            "disposition":         "",
            "last_updated":        None,
            "last_checked":        str(date.today()),
        }
        schedules.append(clean_record_fields(raw_record))

    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(schedules, f, indent=4)

    log.info("Extracted %d schedules → %s", len(schedules), output_json_path)


if __name__ == '__main__':
    input_dir  = Path("../oh")
    output_dir = Path("../../data/oh")

    output_dir.mkdir(parents=True, exist_ok=True)

    input_file  = input_dir / "ohio_schedules.html"
    output_file = output_dir / "general.json"

    if not input_file.exists():
        log.error("Input file not found: %s", input_file)
    else:
        extract_to_json(input_file, output_file)
