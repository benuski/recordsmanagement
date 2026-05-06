import json
import logging
from pathlib import Path
from typing import Dict, Any
from processing.al.config import alabama_config
from processing.al.extractor import parse_alabama_docx

logger = logging.getLogger(__name__)

def run(args, schema: Dict[str, Any]):
    """Run the Alabama pipeline."""
    state_code = args.state_code
    config = alabama_config
    src_dir = Path("processing/al/src")
    output_dir = Path(f"data/{state_code}")
    output_dir.mkdir(parents=True, exist_ok=True)

    docx_files = list(src_dir.glob("*.docx"))
    if not docx_files:
        logger.warning(f"No .docx files found in {src_dir}")
        return

    for docx_path in docx_files:
        schedule_id = docx_path.stem
        logger.info(f"Processing Alabama file: {schedule_id}")
        
        records = parse_alabama_docx(docx_path, schedule_id, schema, config)
        
        if records:
            output_path = output_dir / f"{schedule_id}.json"
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(records, f, indent=2)
            logger.info(f"Saved {len(records)} records to {output_path}")
        else:
            logger.warning(f"No records extracted from {docx_path}")
