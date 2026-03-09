import re
import logging
import pdfplumber
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

def analyze_pdf_preflight(pdf_path: Path) -> tuple[bool, str | None]:
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
    if not word_list:
        return ""
    word_list.sort(key=lambda w: (round(w['top'] / 5) * 5, w['x0']))
    text = " ".join([w['text'] for w in word_list])
    return re.sub(r'-\s+', '', text).strip()
