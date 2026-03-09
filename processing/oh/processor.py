import logging
from pathlib import Path

from processing.oh.harvester import harvest_links, download_detail_pages, download_general_schedule
from processing.oh.parser import process_ohio_html, process_ohio_general_html
from processing.utils import save_records

logger = logging.getLogger(__name__)

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

def parse(args, output_schema: dict):
    """Parses local Ohio HTML files into JSON."""
    logger.info("Initiating Ohio parsing phase...")
    html_files = list(args.input_directory.glob("*.html"))

    if not html_files:
        logger.warning(f"No HTML files found in {args.input_directory} to parse.")
        return

    all_records = []
    for i, file_path in enumerate(html_files):
        if file_path.name.startswith("gen_"):
            records = process_ohio_general_html(file_path, output_schema)
            all_records.extend(records)
        else:
            record = process_ohio_html(file_path, output_schema)
            if record:
                all_records.append(record)

        if (i + 1) % 500 == 0:
            logger.info(f"Parsed {i+1}/{len(html_files)} files...")

    if all_records:
        save_records(all_records, args.output_directory, group_by='agency_name')

    logger.info(f"Done! Successfully parsed {len(html_files)} files to {args.output_directory}")

def run(args, output_schema: dict):
    task = getattr(args, 'task', 'all')

    if task in ['harvest', 'all']:
        if task == 'harvest' or args.update_dl:
            harvest(args)
        elif task == 'all' and not args.update_dl:
            logger.info("Skipping harvest phase (use --task harvest or --update-dl to enable)")

    if task in ['parse', 'all']:
        parse(args, output_schema)
