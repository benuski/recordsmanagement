import logging
import multiprocessing
from functools import partial
from pathlib import Path

from processing.extractor_engine import process_and_evaluate
from processing.va.config import virginia_config

logger = logging.getLogger(__name__)

def load_agency_mapping(csv_path: Path) -> dict[str, str]:
    import csv
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

def run(args, output_schema: dict):
    # Use standardized path for Virginia agencies
    va_agency_csv = Path("processing/va/resources/agencies.csv")
    agency_mapping = load_agency_mapping(va_agency_csv)
    
    active_config = virginia_config
    pdf_files = list(args.input_directory.glob("*.pdf"))

    if not pdf_files:
        logger.warning(f"No PDF files found in {args.input_directory}")
        return
        
    logger.info(f"Starting pipeline for {len(pdf_files)} files using VA configuration.")
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
