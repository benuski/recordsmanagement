from processing.central_file import clean_record_fields
from processing.utils.pdf_utils import stringify_words # If some still expect it here

def split_title_and_description(raw_text: str) -> tuple[str, str]:
    import re
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
