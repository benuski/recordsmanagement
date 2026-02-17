import pdfplumber
import json
from pathlib import Path

def diagnose_pdf(pdf_path):
    """Diagnose what pdfplumber can see in the PDF"""
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            # Check for text
            text = page.extract_text()

            # Try default table extraction
            tables = page.extract_tables()

            # Try with adjusted settings
            tables_adjusted = page.extract_tables(table_settings={
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "explicit_vertical_lines": [],
                "explicit_horizontal_lines": [],
                "snap_tolerance": 3,
                "join_tolerance": 3,
                "edge_min_length": 3,
                "min_words_vertical": 3,
                "min_words_horizontal": 1,
            })

if __name__ == "__main__":
    pdf_file = "GS-102.pdf"
    diagnose_pdf(pdf_file)
