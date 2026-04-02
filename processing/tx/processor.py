import logging
from pathlib import Path
from processing.tx.parser import process_texas_pdf, parse_agencies_html, load_retention_codes
from processing.central_file import save_records
from processing.extractor_engine import run_state_pipeline

logger = logging.getLogger(__name__)

def tx_worker(pdf_path: Path, output_dir: Path, agency_mapping: dict, schema: dict, config, skip_ocr: bool):
    """Worker wrapper for Texas retention schedules."""
    retention_codes_path = Path("processing/resources/retention_codes.csv")
    try:
        records = process_texas_pdf(pdf_path, schema, retention_codes_path, agency_mapping)
        if records:
            # Texas typically groups by schedule_id, but per-file is equivalent here
            save_records(records, output_dir, default_filename=f"{pdf_path.stem}.json")
            logger.info(f"Extracted {len(records)} records from {pdf_path.name}")
    except Exception as e:
        logger.error(f"Failed to process TX file {pdf_path.name}: {e}")

def run(args, output_schema: dict):
    """Entry point for TX pipeline using the standardized runner."""
    from processing.tx.config import texas_config
    
    # Pre-load agency mapping for the workers
    agencies_html_path = Path("processing/tx/src/agencies.html")
    agency_mapping = {}
    if agencies_html_path.exists():
        agency_mapping = parse_agencies_html(agencies_html_path)
        logger.info(f"Loaded {len(agency_mapping)} agencies from {agencies_html_path}")

    # Standard runner will handle the glob and the pool
    run_state_pipeline(
        args, 
        texas_config, 
        output_schema, 
        worker_func=tx_worker,
        agency_mapping=agency_mapping
    )
