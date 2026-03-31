import re
import logging
import pdfplumber
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

def analyze_pdf_preflight(pdf_path: Path) -> tuple[bool, str | None]:
    """Determines if a PDF is an image scan and extracts the effective date if possible."""
    is_image = True
    eff_date = None
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                return is_image, eff_date

            pages_to_check = min(2, len(pdf.pages))
            for i in range(pages_to_check):
                text = pdf.pages[i].extract_text(x_tolerance=2, y_tolerance=2)
                if text and text.strip():
                    is_image = False 
                    if eff_date is None:
                        match = re.search(r'(?i)EFFECTIVE\s+(?:SCHEDULE\s+)?DATE[:\s]+(\d{1,2}/\d{1,2}/\d{4})', text)
                        if match:
                            eff_date = datetime.strptime(match.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
                    if eff_date:
                        break
    except Exception as e:
        logger.warning(f"Could not complete pre-flight check for {pdf_path}: {e}")

    return is_image, eff_date

def stringify_words(word_list: list[dict]) -> str:
    """Converts a list of pdfplumber word dicts into a single cleaned string."""
    if not word_list:
        return ""
    # Sort by approximate line (top rounded to nearest 5) and then left-to-right
    word_list.sort(key=lambda w: (round(w['top'] / 5) * 5, w['x0']))
    text = " ".join([w['text'] for w in word_list])
    # Clean up hyphenated line breaks
    return re.sub(r'-\s+', '', text).strip()

def split_title_and_description(raw_text: str) -> tuple[str, str]:
    """Splits a combined text block into a Title and a Description based on common triggers."""
    match = re.search(
        r'((?:This series\s+)?(?:documents|Collects|Verifies|Consists|consists)\b.*)',
        raw_text, re.IGNORECASE
    )
    if match:
        return raw_text[:match.start()].strip(), match.group(1).strip()
    
    # Fallback: split on first period if the first part is short
    parts = raw_text.split('.', 1)
    if len(parts) > 1 and len(parts[0]) < 100:
        return parts[0].strip(), parts[1].strip()
        
    return raw_text.strip(), ""
