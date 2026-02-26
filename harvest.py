import argparse
import logging
import multiprocessing
from pathlib import Path
from functools import partial
import csv
import json

from processing.extractor_engine import process_and_evaluate 
from processing.va.virginia import virginia_config

# Import Ohio modules
from processing.oh.ohio import ohio_config
from processing.oh.harvester import harvest_links, download_detail_pages
from processing.oh.parser import process_ohio_html

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
    parser = argparse.ArgumentParser(description="Extract data from state records retention schedules.")
    parser.add_argument("--input-directory", required=True, type=Path, help="Path to the directory containing source PDFs or to save/read HTML files")
    parser.add_argument("--output-directory", required=True, type=Path, help="Path to save the resulting JSON files")
    parser.add_argument("--state-code", required=True, type=str, choices=["va", "oh"], help="The two-letter state code (e.g., va, oh)")
    parser.add_argument("--schema-path", type=Path, default=Path("processing/output_template_clean.json"), help="Path to the output JSON schema")
    parser.add_argument("--agency-csv", type=Path, default=Path("agencies.csv"), help="Path to the agency mapping CSV")
    parser.add_argument("--skip-ocr", action="store_true", help="Bypass the marker-pdf OCR engine and skip image-only PDFs")
    
    args = parser.parse_args()

    # Ensure directories exist
    args.output_directory.mkdir(parents=True, exist_ok=True)
    args.input_directory.mkdir(parents=True, exist_ok=True) 
    
    agency_mapping = load_agency_mapping(args.agency_csv)
    output_schema = load_output_schema(args.schema_path)

    # -----------------------------------------------------------------------
    # Virginia Pipeline (PDFs)
    # -----------------------------------------------------------------------
    if args.state_code == "va":
        active_config = virginia_config
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
                skip_ocr=args.skip_ocr  
            )

            ctx = multiprocessing.get_context('spawn')
            with ctx.Pool(processes=1, maxtasksperchild=25) as pool:
                pool.map(worker, pdf_files)

    # -----------------------------------------------------------------------
    # Ohio Pipeline (HTML Web Scraping)
    # -----------------------------------------------------------------------
    elif args.state_code == "oh":
        logger.info(f"Starting pipeline using {args.state_code.upper()} configuration.")
        base_ohio_url = "https://rims.das.ohio.gov"
        
        # 1. Harvest & Download
        logger.info("Initiating Ohio harvest and download phase...")
        urls = harvest_links(base_ohio_url)
        if urls:
            # We use input_directory as the staging ground for raw HTML files
            download_detail_pages(urls, args.input_directory)
            
        # 2. Parse
        logger.info("Initiating Ohio parsing phase...")
        html_files = list(args.input_directory.glob("*.html"))
        schedules = []
        
        if not html_files:
            logger.warning(f"No HTML files found in {args.input_directory} to parse.")
        else:
            for i, file_path in enumerate(html_files):
                record = process_ohio_html(file_path, output_schema)
                if record:
                    schedules.append(record)
                    
                if (i + 1) % 500 == 0:
                    logger.info(f"Parsed {i+1}/{len(html_files)} files...")

            output_file = args.output_directory / "ohio_records.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(schedules, f, indent=4)
                
            logger.info(f"Done! Successfully extracted {len(schedules)} records to {output_file}")