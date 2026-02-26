import argparse
import logging
import multiprocessing
from pathlib import Path
from functools import partial
import csv
import json

from extractor_engine import process_and_evaluate 
from processing.va.virginia import virginia_config

# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------
def load_agency_mapping(csv_path: Path) -> dict[str, str]:
    mapping = {}
    if not csv_path.exists():
        logger.warning(f"Agency CSV not found at {csv_path}. Agency names will be None.")
        return mapping
    try:
        with open(csv_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get("Agency Code", "").strip()
                name = row.get("Agency Name", "").strip()
                if code and name:
                    mapping[code] = name
    except Exception as e:
        logger.error(f"Failed to load agency mapping: {e}")
    return mapping

def load_output_schema(schema_path: Path) -> dict:
    if not schema_path.exists():
        logger.warning(f"Output schema not found at {schema_path}. Records may have missing fields.")
        return {}
    try:
        with open(schema_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load output schema: {e}")
        return {}

# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract data from state records retention PDFs.")
    parser.add_argument("--input-directory", required=True, type=Path, help="Path to the directory containing source PDFs")
    parser.add_argument("--output-directory", required=True, type=Path, help="Path to save the resulting JSON files")
    parser.add_argument("--state-code", required=True, type=str, choices=["va"], help="The two-letter state code (e.g., va)")
    parser.add_argument("--schema-path", type=Path, default=Path("output_template_clean.json"), help="Path to the output JSON schema")
    parser.add_argument("--agency-csv", type=Path, default=Path("agencies.csv"), help="Path to the agency mapping CSV")
    
    # New long-form flag to bypass OCR
    parser.add_argument("--skip-ocr", action="store_true", help="Bypass the marker-pdf OCR engine and skip image-only PDFs")
    
    args = parser.parse_args()

    if args.state_code == "va":
        active_config = virginia_config
    else:
        logger.error(f"Configuration for state '{args.state_code}' not found.")
        exit(1)

    args.output_directory.mkdir(parents=True, exist_ok=True)
    
    agency_mapping = load_agency_mapping(args.agency_csv)
    output_schema = load_output_schema(args.schema_path)

    pdf_files = list(args.input_directory.glob("*.pdf"))

    if not pdf_files:
        logger.warning(f"No PDF files found in {args.input_directory}")
    else:
        logger.info(f"Starting pipeline for {len(pdf_files)} files using {args.state_code.upper()} configuration.")
        if args.skip_ocr:
            logger.info("OCR skipping is ENABLED. Image-only PDFs will be ignored.")
        
        worker = partial(
            process_and_evaluate,
            output_dir=args.output_directory,
            agency_mapping=agency_mapping,
            schema=output_schema,
            config=active_config,
            skip_ocr=args.skip_ocr  # Pass the flag to the engine
        )

        ctx = multiprocessing.get_context('spawn')
        with ctx.Pool(processes=1, maxtasksperchild=25) as pool:
            pool.map(worker, pdf_files)