import gc
import json
import re
import logging
from pathlib import Path

from processing.base_config import StateScheduleConfig
from processing.utils import (
    analyze_pdf_preflight,
    score_records,
    select_optimal_strategy_memory_aware,
    parse_using_marker_html_optimized,
    parse_using_table_engine,
    parse_using_vertical_silo,
    save_records
)

logger = logging.getLogger(__name__)

def process_and_evaluate(pdf_path: Path, output_dir: Path, agency_mapping: dict, schema: dict, config: StateScheduleConfig, skip_ocr: bool = False) -> None:
    pdf_path = Path(pdf_path)
    schedule_id = pdf_path.stem

    match = re.match(r'^(\d+)', schedule_id)
    agency_code = match.group(1) if match else schedule_id[:3]
    agency_name = agency_mapping.get(agency_code, None)

    try:
        is_image, effective_date = analyze_pdf_preflight(pdf_path)
        
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
                    pdf_path, schedule_id, effective_date, is_image, schema, config
                )
            elif strategy == 'table':
                records = parse_using_table_engine(
                    pdf_path, schedule_id, effective_date, schema, config
                )
            elif strategy == 'silo':
                records = parse_using_vertical_silo(
                    pdf_path, schedule_id, effective_date, schema, config
                )

            for record in records:
                record['agency_name'] = agency_name

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
