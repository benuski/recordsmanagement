import logging
from processing.nc.parser import process_nc_pdf
from processing.utils import save_records

logger = logging.getLogger(__name__)

def run(args, output_schema: dict):
    pdf_files = list(args.input_directory.glob("??_*.pdf"))
    
    if not pdf_files:
        logger.warning(f"No PDF files found in {args.input_directory} matching '??_*.pdf'")
        return

    logger.info(f"Starting pipeline for {len(pdf_files)} files using NC configuration.")
    
    all_records = []
    for pdf_file in pdf_files:
        logger.info(f"Processing {pdf_file.name}...")
        try:
            records = process_nc_pdf(pdf_file, output_schema)
            all_records.extend(records)
            logger.info(f"Extracted {len(records)} records from {pdf_file.name}")
        except Exception as e:
            logger.error(f"Failed to process {pdf_file.name}: {e}")
            
    if all_records:
        save_records(all_records, args.output_directory, default_filename="functional_schedule.json")
        logger.info(f"Done! Successfully processed {len(all_records)} records to {args.output_directory}")
