import argparse
import logging
import json
import importlib
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
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
    parser.add_argument("--state-code", required=True, type=str, choices=["va", "oh", "tx", "nc"], help="The two-letter state code (e.g., va, oh, tx, nc)")
    parser.add_argument("--task", type=str, choices=["harvest", "parse", "all"], default="all", help="The task to perform: harvest (download), parse (extract), or all")
    parser.add_argument("--schema-path", type=Path, default=Path("processing/output_template_clean.json"), help="Path to the output JSON schema")
    parser.add_argument("--agency-csv", type=Path, default=Path("agencies.csv"), help="Path to the agency mapping CSV")
    parser.add_argument("--skip-ocr", action="store_true", help="Bypass the marker-pdf OCR engine and skip image-only PDFs")
    parser.add_argument("--update-dl", action="store_true", help="Download/update HTML files from remote servers (default: parse existing files only)")

    args = parser.parse_args()

    if args.input_directory is None:
        args.input_directory = Path("processing") / args.state_code / "src"

    if args.output_directory is None:
        args.output_directory = Path("data") / args.state_code

    args.output_directory.mkdir(parents=True, exist_ok=True)
    args.input_directory.mkdir(parents=True, exist_ok=True) 
    
    output_schema = load_output_schema(args.schema_path)

    # Dynamically load the state processor
    try:
        module_path = f"processing.{args.state_code}.processor"
        state_module = importlib.import_module(module_path)
        
        if hasattr(state_module, 'run'):
            state_module.run(args, output_schema)
        else:
            logger.error(f"Module {module_path} does not have a 'run' function.")
    except ImportError as e:
        logger.error(f"Failed to import processor for state {args.state_code}: {e}")
    except Exception as e:
        logger.error(f"An error occurred during execution of {args.state_code} pipeline: {e}")
        import traceback
        logger.error(traceback.format_exc())
