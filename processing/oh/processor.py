import logging
from pathlib import Path
from processing.oh.harvester import harvest_links, download_detail_pages, download_general_schedule
from processing.oh.parser import process_ohio_html, process_ohio_general_html
from processing.central_file import save_records, get_nested_val
from processing.extractor_engine import run_state_pipeline

logger = logging.getLogger(__name__)

def oh_worker(file_path: Path, output_dir: Path, agency_mapping: dict, schema: dict, config, skip_ocr: bool):
    """Worker wrapper for Ohio HTML schedules."""
    try:
        if file_path.name.startswith("gen_"):
            records = process_ohio_general_html(file_path, schema)
        else:
            record = process_ohio_html(file_path, schema)
            records = [record] if record else []
            
        if records:
            # Ohio specific schedules are 1-per-file usually, but we want to group by agency eventually.
            # For the parallel worker, we save per input file to avoid collisions.
            # Consolidation can happen in a post-process step if needed.
            save_records(records, output_dir, default_filename=f"{file_path.stem}.json")
    except Exception as e:
        logger.error(f"Failed to process OH file {file_path.name}: {e}")

def harvest(args):
    """Downloads Ohio records from the remote server."""
    logger.info("Initiating Ohio harvest phase...")
    base_ohio_url = "https://rims.das.ohio.gov"
    urls = harvest_links(base_ohio_url)

    if urls:
        active_ids = {url.split('/')[-1] for url in urls}
        existing_html_files = list(args.input_directory.glob("spec_*.html"))
        pruned_count = 0

        for file_path in existing_html_files:
            record_id = file_path.stem.replace("spec_", "")
            if record_id not in active_ids:
                logger.info(f"[{record_id}] Record no longer active. Deleting obsolete HTML.")
                file_path.unlink()
                pruned_count += 1

        if pruned_count > 0:
            logger.info(f"Pruned {pruned_count} obsolete records from the local directories.")

        logger.info("Downloading Ohio general schedules...")
        download_general_schedule(base_ohio_url, args.input_directory)

        logger.info("Downloading missing specific records...")
        download_detail_pages(urls, args.input_directory)

def run(args, output_schema: dict):
    task = getattr(args, 'task', 'all')

    if task in ['harvest', 'all']:
        if task == 'harvest' or args.update_dl:
            harvest(args)
        elif task == 'all' and not args.update_dl:
            logger.info("Skipping harvest phase (use --task harvest or --update-dl to enable)")

    if task in ['parse', 'all']:
        from processing.oh.config import ohio_config
        # Run Ohio parsing in parallel!
        run_state_pipeline(
            args, 
            ohio_config, 
            output_schema, 
            glob_pattern="*.html", 
            worker_func=oh_worker
        )
