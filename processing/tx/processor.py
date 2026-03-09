import logging
from pathlib import Path

from processing.tx.tx_pdf_processor import process_texas_pdf
from processing.tx.parse_agencies import parse_agencies_html
from processing.utils import save_records

logger = logging.getLogger(__name__)

def run(args, output_schema: dict):
    pdf_files = list(args.input_directory.glob("*.pdf"))
    retention_codes_path = Path("processing/tx/resources/retentioncodes.csv")
    agencies_html_path = Path("processing/tx/src/agencies.html")

    # Load agency mapping from agencies.html
    agency_mapping = {}
    if agencies_html_path.exists():
        agency_mapping = parse_agencies_html(agencies_html_path)
        logger.info(f"Loaded {len(agency_mapping)} agencies from {agencies_html_path}")
    else:
        logger.warning(f"agencies.html not found at {agencies_html_path}")

    if not pdf_files:
        logger.warning(f"No PDF files found in {args.input_directory}")
        return

    logger.info(f"Starting pipeline for {len(pdf_files)} files using TX configuration.")

    all_records = []

    for pdf_file in pdf_files:
        logger.info(f"Processing {pdf_file.name}...")
        try:
            records = process_texas_pdf(pdf_file, output_schema, retention_codes_path, agency_mapping)
            all_records.extend(records)
            logger.info(f"Extracted {len(records)} records from {pdf_file.name}")
        except Exception as e:
            logger.error(f"Failed to process {pdf_file.name}: {e}")

    if all_records:
        save_records(all_records, args.output_directory, group_by='schedule_id')

    logger.info(f"Done! Successfully processed {len(pdf_files)} files to {args.output_directory}")
