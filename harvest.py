import argparse
import logging
import json
import sys
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------
def setup_logging(state_code: str):
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{state_code}_{timestamp}.log"
    
    # Configure root logger to capture all module logs
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    
    # Terminal Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File Handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    return log_file

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------
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
    parser.add_argument("--state-code", required=True, type=str, choices=["va", "oh", "tx", "nc", "al"], help="The two-letter state code (e.g., va, oh, tx, nc, al)")
    parser.add_argument("--task", type=str, choices=["harvest", "parse", "all"], default="all", help="The task to perform: harvest (download), parse (extract), or all")
    parser.add_argument("--schema-path", type=Path, default=Path("processing/output_template_clean.json"), help="Path to the output JSON schema")
    parser.add_argument("--agency-csv", type=Path, default=Path("agencies.csv"), help="Path to the agency mapping CSV")
    parser.add_argument("--skip-ocr", action="store_true", help="Bypass the marker-pdf OCR engine and skip image-only PDFs")
    parser.add_argument("--update-dl", action="store_true", help="Download/update HTML files from remote servers (default: parse existing files only)")

    args = parser.parse_args()

    log_path = setup_logging(args.state_code)
    logger.info(f"Logging initialized. Saving to {log_path}")

    if args.input_directory is None:
        args.input_directory = Path("processing") / args.state_code / "src"

    if args.output_directory is None:
        args.output_directory = Path("data") / args.state_code

    args.output_directory.mkdir(parents=True, exist_ok=True)
    args.input_directory.mkdir(parents=True, exist_ok=True) 
    
    output_schema = load_output_schema(args.schema_path)

    from processing.registry import STATE_REGISTRY
    from processing.extractor_engine import run_state_pipeline

    if args.state_code not in STATE_REGISTRY:
        logger.error(f"State {args.state_code} not found in registry.")
        sys.exit(1)

    state_entry = STATE_REGISTRY[args.state_code]
    
    if state_entry['runner']:
        # State has a custom runner (e.g., OH, TX, NC)
        state_entry['runner'](args, output_schema)
    else:
        # State uses the standard extractor_engine pipeline (e.g., VA)
        run_state_pipeline(args, state_entry['config'], output_schema)
