import argparse
import logging
import multiprocessing
from pathlib import Path
from functools import partial
import csv
import json

from processing.extractor_engine import process_and_evaluate
from processing.va.virginia import virginia_config
from processing.tx.texas import texas_config
from processing.tx.tx_pdf_processor import process_texas_pdf

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
    parser.add_argument("--input-directory", type=Path, default=None, help="Path to the directory containing source PDFs or to save/read HTML files (default: processing/<state-code>/src/)")
    parser.add_argument("--output-directory", type=Path, default=None, help="Path to save the resulting JSON files (default: data/<state-code>/)")
    parser.add_argument("--state-code", required=True, type=str, choices=["va", "oh", "tx"], help="The two-letter state code (e.g., va, oh, tx)")
    parser.add_argument("--schema-path", type=Path, default=Path("processing/output_template_clean.json"), help="Path to the output JSON schema")
    parser.add_argument("--agency-csv", type=Path, default=Path("agencies.csv"), help="Path to the agency mapping CSV")
    parser.add_argument("--skip-ocr", action="store_true", help="Bypass the marker-pdf OCR engine and skip image-only PDFs")
    parser.add_argument("--update-dl", action="store_true", help="Download/update HTML files from remote servers (default: parse existing files only)")

    args = parser.parse_args()

    if args.input_directory is None:
        args.input_directory = Path("processing") / args.state_code / "src"

    if args.output_directory is None:
        args.output_directory = Path("data") / args.state_code

    args.output_directory.mkdir(parents=True, exist_ok=True)
    args.input_directory.mkdir(parents=True, exist_ok=True) 
    
    output_schema = load_output_schema(args.schema_path)

    # -----------------------------------------------------------------------
    # Virginia Pipeline (PDFs)
    # -----------------------------------------------------------------------
    if args.state_code == "va":
        agency_mapping = load_agency_mapping(args.agency_csv)
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
    # Texas Pipeline (PDFs)
    # -----------------------------------------------------------------------
    elif args.state_code == "tx":
        pdf_files = list(args.input_directory.glob("*.pdf"))
        retention_codes_path = Path("processing/tx/retentioncodes.csv")
        agencies_html_path = Path("processing/tx/src/agencies.html")

        # Load agency mapping from agencies.html
        from processing.tx.parse_agencies import parse_agencies_html
        agency_mapping = {}
        if agencies_html_path.exists():
            agency_mapping = parse_agencies_html(agencies_html_path)
            logger.info(f"Loaded {len(agency_mapping)} agencies from {agencies_html_path}")
        else:
            logger.warning(f"agencies.html not found at {agencies_html_path}")

        if not pdf_files:
            logger.warning(f"No PDF files found in {args.input_directory}")
        else:
            logger.info(f"Starting pipeline for {len(pdf_files)} files using {args.state_code.upper()} configuration.")

            all_records = []

            for pdf_file in pdf_files:
                logger.info(f"Processing {pdf_file.name}...")
                try:
                    records = process_texas_pdf(pdf_file, output_schema, retention_codes_path, agency_mapping)
                    all_records.extend(records)
                    logger.info(f"Extracted {len(records)} records from {pdf_file.name}")
                except Exception as e:
                    logger.error(f"Failed to process {pdf_file.name}: {e}")

            # Group records by schedule_id (or use 'general' if no schedule_id)
            from collections import defaultdict
            grouped_records = defaultdict(list)

            for record in all_records:
                schedule_id = record.get('schedule_id', '').strip()
                if schedule_id:
                    grouped_records[schedule_id].extend([record])
                else:
                    grouped_records['general'].append(record)

            # Write grouped records to files
            for schedule_id, records in grouped_records.items():
                if records:
                    output_file = args.output_directory / f"{schedule_id}.json"
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(records, f, indent=2, ensure_ascii=False)
                    logger.info(f"Wrote {len(records)} records to {output_file}")

            logger.info(f"Done! Successfully extracted {len(all_records)} records from {len(pdf_files)} files to {args.output_directory}")

    # -----------------------------------------------------------------------
    # Ohio Pipeline (HTML Web Scraping)
    # -----------------------------------------------------------------------
    elif args.state_code == "oh":
        logger.info(f"Starting pipeline using {args.state_code.upper()} configuration.")
        base_ohio_url = "https://rims.das.ohio.gov"

        # 1. Harvest, Prune & Download (only if --update-dl is set)
        if args.update_dl:
            logger.info("Initiating Ohio harvest phase...")
            urls = harvest_links(base_ohio_url)

            if urls:
                active_ids = {url.split('/')[-1] for url in urls}
                existing_html_files = list(args.input_directory.glob("spec_*.html"))
                pruned_count = 0

                for file_path in existing_html_files:
                    # Extract ID from spec_12345.html -> 12345
                    record_id = file_path.stem.replace("spec_", "")
                    if record_id not in active_ids:
                        logger.info(f"[{record_id}] Record no longer active. Deleting obsolete HTML.")

                        # Delete the obsolete raw HTML
                        file_path.unlink()

                        pruned_count += 1

                if pruned_count > 0:
                    logger.info(f"Pruned {pruned_count} obsolete records from the local directories.")

                # 2. Download general schedules
                from processing.oh.harvester import download_general_schedule
                logger.info("Downloading Ohio general schedules...")
                download_general_schedule(base_ohio_url, args.input_directory)

                # 3. Download missing active specific records
                download_detail_pages(urls, args.input_directory)
        else:
            logger.info("Skipping download phase (--update-dl not set)")

        # 4. Parse
        logger.info("Initiating Ohio parsing phase...")
        html_files = list(args.input_directory.glob("*.html"))

        if not html_files:
            logger.warning(f"No HTML files found in {args.input_directory} to parse.")
        else:
            from processing.oh.parser import process_ohio_html, process_ohio_general_html
            from collections import defaultdict

            # Group records by agency_name
            grouped_records = defaultdict(list)

            for i, file_path in enumerate(html_files):
                # Route to the appropriate parser
                if file_path.name.startswith("gen_"):
                    records = process_ohio_general_html(file_path, output_schema)
                    # General schedules go to a special file
                    grouped_records['general'].extend(records)
                else:
                    record = process_ohio_html(file_path, output_schema)
                    if record:
                        # Group by agency_name (e.g., "OEB", "LCC", "DAS")
                        agency_name = record.get('agency_name', '').strip()
                        if agency_name:
                            grouped_records[agency_name].append(record)
                        else:
                            logger.warning(f"No agency_name found in {file_path.name}")

                if (i + 1) % 500 == 0:
                    logger.info(f"Parsed {i+1}/{len(html_files)} files...")

            # Write grouped records to files
            for agency_name, records in grouped_records.items():
                if records:
                    output_file = args.output_directory / f"{agency_name}.json"
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(records, f, indent=2, ensure_ascii=False)

            logger.info(f"Done! Successfully extracted {len(grouped_records)} schedule groups from {len(html_files)} files to {args.output_directory}")