import gc
import json
import re
import logging
import multiprocessing
import csv
from pathlib import Path
from functools import partial

from processing.base_config import StateScheduleConfig
from processing.core import analyze_pdf_preflight
from processing.strategies import (
    select_optimal_strategy_memory_aware,
    parse_using_marker_html_optimized,
    parse_using_table_engine,
    parse_using_vertical_silo,
)
from processing.central_file import save_records, score_records

logger = logging.getLogger(__name__)

# Global semaphore for GPU access (initialized in run_state_pipeline)
_gpu_semaphore = None

def init_worker(semaphore):
    """Initializes the worker process with a global semaphore."""
    global _gpu_semaphore
    _gpu_semaphore = semaphore

def load_agency_mapping(state_code: str) -> dict[str, str]:
    """Loads state-specific agency mapping CSV if it exists."""
    mapping = {}
    csv_path = Path(f"processing/{state_code}/resources/agencies.csv")
    
    if not csv_path.exists():
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
        logger.error(f"Failed to load agency mapping for {state_code}: {e}")
    return mapping

def process_and_evaluate(pdf_path: Path, output_dir: Path, agency_mapping: dict, schema: dict, config: StateScheduleConfig, skip_ocr: bool = False) -> None:
    pdf_path = Path(pdf_path)
    schedule_id = pdf_path.stem

    match = re.match(r'^(\d+)', schedule_id)
    agency_code = match.group(1) if match else schedule_id[:3]
    agency_name = agency_mapping.get(agency_code, None)

    try:
        is_image, effective_date = analyze_pdf_preflight(pdf_path)
        
        # Determine source URL if base_url is provided in config
        source_url = ""
        if config.base_url:
            source_url = f"{config.base_url.rstrip('/')}/{pdf_path.name}"

        if is_image and skip_ocr:
            logger.warning(f"[{schedule_id}] File is an image scan and --skip-ocr is enabled. Skipping entirely.")
            return

        strategies = select_optimal_strategy_memory_aware(pdf_path, is_image)
        
        if skip_ocr and 'html' in strategies:
            strategies.remove('html')
            
        if not strategies:
            logger.warning(f"[{schedule_id}] No valid parsers available after skipping OCR.")
            return

        best_score = -9999
        best_records = []
        winning_method = "None"

        for strategy in strategies:
            logger.info(f"[{schedule_id}] Attempting strategy: {strategy.upper()}")
            records = []

            if strategy == 'html':
                records = parse_using_marker_html_optimized(
                    pdf_path, schedule_id, effective_date, is_image, schema, config,
                    gpu_semaphore=_gpu_semaphore
                )
            elif strategy == 'table':
                records = parse_using_table_engine(
                    pdf_path, schedule_id, effective_date, schema, config
                )
            elif strategy == 'silo':
                records = parse_using_vertical_silo(
                    pdf_path, schedule_id, effective_date, schema, config
                )

            from processing.central_file import set_nested_val
            for record in records:
                set_nested_val(record, 'agency_name', agency_name)
                if source_url:
                    set_nested_val(record, 'url', source_url)

            score = score_records(records, config)

            if score > best_score:
                best_score = score
                best_records = records
                winning_method = strategy.upper()

            if len(records) > 0 and score >= (len(records) * 10):
                logger.info(
                    f"[{schedule_id}] Early termination triggered: {strategy.upper()} "
                    f"achieved a penalty-free extraction."
                )
                break

            del records
            gc.collect()

        if not best_records:
            logger.warning(f"[{schedule_id}] No text could be extracted at all. Saving empty array.")
            best_records = []
        elif best_score <= 0:
            logger.warning(f"[{schedule_id}] Extraction scored poorly ({best_score}). Overwriting file anyway for review.")

        save_records(best_records, output_dir, default_filename=f"{schedule_id}.json")

        logger.info(
            f"Processed: {schedule_id}.pdf -> {len(best_records)} records. "
            f"(Winner: {winning_method} | Score: {best_score})"
        )

    except Exception as e:
        logger.error(f"Failed to process {pdf_path.name}: {e}", exc_info=True)

def run_state_pipeline(args, state_config: StateScheduleConfig, output_schema: dict, glob_pattern: str = "*.pdf", worker_func=None):
    """Standardized entry point for state processing."""
    agency_mapping = load_agency_mapping(args.state_code)
    
    files = list(args.input_directory.glob(glob_pattern))
    if not files:
        logger.warning(f"No files found in {args.input_directory} matching {glob_pattern}")
        return
        
    logger.info(f"Starting pipeline for {len(files)} files using {args.state_code.upper()} configuration.")
    
    if worker_func is None:
        worker_func = process_and_evaluate

    # Note: worker_func must accept (file_path, output_dir, agency_mapping, schema, config, skip_ocr)
    worker = partial(
        worker_func,
        output_dir=args.output_directory,
        agency_mapping=agency_mapping,
        schema=output_schema,
        config=state_config,
        skip_ocr=args.skip_ocr
    )

    # Use a Manager to create a semaphore that can be shared across processes
    manager = multiprocessing.Manager()
    gpu_sem = manager.Semaphore(1) # Lock to 1 GPU instance

    # Use 'spawn' for safe multiprocessing with complex libraries
    ctx = multiprocessing.get_context('spawn')
    # Use fewer processes than CPU count to be safe with memory
    num_procs = max(1, multiprocessing.cpu_count() // 2)
    with ctx.Pool(
        processes=num_procs, 
        maxtasksperchild=25,
        initializer=init_worker,
        initargs=(gpu_sem,)
    ) as pool:
        pool.map(worker, files)
