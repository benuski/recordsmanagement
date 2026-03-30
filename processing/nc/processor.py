import logging
from pathlib import Path
from processing.nc.parser import process_nc_pdf
from processing.central_file import save_records
from processing.extractor_engine import run_state_pipeline

logger = logging.getLogger(__name__)

def nc_worker(pdf_path: Path, output_dir: Path, agency_mapping: dict, schema: dict, config, skip_ocr: bool):
    """Worker wrapper for North Carolina functional schedules."""
    try:
        logger.info(f"Processing NC file: {pdf_path.name}")
        records = process_nc_pdf(pdf_path, schema)
        if records:
            save_records(records, output_dir, default_filename=f"{pdf_path.stem}.json")
            logger.info(f"Extracted {len(records)} records from {pdf_path.name}")
    except Exception as e:
        logger.error(f"Failed to process NC file {pdf_path.name}: {e}")

def run(args, output_schema: dict):
    """Entry point for NC pipeline using the standardized runner."""
    from processing.nc.config import nc_config
    run_state_pipeline(
        args, 
        nc_config, 
        output_schema, 
        glob_pattern="??_*.pdf", 
        worker_func=nc_worker
    )
