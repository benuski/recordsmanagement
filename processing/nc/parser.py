import json
import re
import subprocess
import logging
from pathlib import Path
from datetime import date
import os

from processing.utils import make_record, clean_record_fields
from processing.base_config import StateScheduleConfig

logger = logging.getLogger(__name__)

from processing.nc.config import nc_config

def get_text(node):
    texts = []
    if isinstance(node, dict):
        if 'text' in node:
            texts.extend(node['text'])
        if 'children' in node:
            for child in node['children']:
                texts.extend(get_text(child))
    elif isinstance(node, list):
        for item in node:
            texts.extend(get_text(item))
    return texts

def get_paragraph_texts(node):
    paragraphs = []
    if isinstance(node, dict):
        if node.get('type') == 'P':
            text = ''.join(get_text(node)).strip()
            text = re.sub(r'\s+', ' ', text)
            if text:
                paragraphs.append(text)
        if 'children' in node:
            for child in node['children']:
                paragraphs.extend(get_paragraph_texts(child))
    elif isinstance(node, list):
        for item in node:
            paragraphs.extend(get_paragraph_texts(item))
    return paragraphs

def find_rows(node):
    rows = []
    if isinstance(node, dict):
        if node.get('type') == 'TR':
            rows.append(node)
        if 'children' in node:
            for child in node['children']:
                rows.extend(find_rows(child))
    elif isinstance(node, list):
        for item in node:
            rows.extend(find_rows(item))
    return rows

def ensure_json_structure(pdf_path: Path) -> Path:
    """Runs pdfplumber --structure-text if JSON doesn't exist."""
    json_path = pdf_path.with_suffix('.json')
    if not json_path.exists():
        logger.info(f"Generating structure JSON for {pdf_path.name}...")
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        try:
            # We use pdfplumber CLI to dump the structure tree
            with open(json_path, 'w', encoding='utf-8') as f:
                subprocess.run(
                    ["pixi", "run", "pdfplumber", str(pdf_path), "--structure-text"],
                    stdout=f,
                    env=env,
                    check=True
                )
        except Exception as e:
            logger.error(f"Failed to generate JSON for {pdf_path.name}: {e}")
            if json_path.exists():
                json_path.unlink() # remove partial/failed file
    return json_path

def parse_transfer_instructions(data) -> dict:
    texts = get_paragraph_texts(data)
    instructions = {}
    in_transfer = False
    for text in texts:
        if 'Records That Will Transfer to the State Records Center' in text:
            in_transfer = True
            continue
        if in_transfer:
            if text == 'Appendix' or text == 'Agency Series Title Item Number':
                break
            if re.match(r'^\d+\.[A-Z0-9]+$', text):
                continue
            
            match = re.match(r'^(\d+\.[A-Z0-9]+)\s+(.+?):\s+(.+)$', text)
            if match:
                rc_id = match.group(1)
                instr = match.group(3)
                if rc_id in instructions:
                    instructions[rc_id] += ' ; ' + instr
                else:
                    instructions[rc_id] = instr
    return instructions

def parse_appendix_mappings(trs) -> list[dict]:
    appendix_records = []
    in_appendix = False
    current_agency = ''
    
    for tr in trs:
        cells = []
        for td in tr.get('children', []):
            text = ''.join(get_text(td)).strip()
            text = re.sub(r'\s+', ' ', text)
            if text: cells.append(text)
        
        if not cells: continue
        
        if cells == ['Agency', 'Series Title', 'Item Number'] or cells == ['Appendix', 'Agency', 'Series Title', 'Item Number']:
            in_appendix = True
            continue
            
        if 'Function No.' in cells:
            break

        if in_appendix:
            if len(cells) >= 3:
                current_agency = cells[0]
                series_title = cells[1]
                item_number = cells[2]
                appendix_records.append({'agency': current_agency, 'legacy_title': series_title, 'item_number': item_number})
            elif len(cells) == 2:
                if re.match(r'^\d+$', cells[1]):
                    series_title = cells[0]
                    item_number = cells[1]
                    appendix_records.append({'agency': current_agency, 'legacy_title': series_title, 'item_number': item_number})
    
    return appendix_records

def process_nc_pdf(pdf_path: Path, schema: dict) -> list[dict]:
    schedule_id = pdf_path.stem.split('_')[0] if '_' in pdf_path.stem else pdf_path.stem
    json_path = ensure_json_structure(pdf_path)
    
    if not json_path.exists():
        return []

    with open(json_path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse JSON for {json_path.name}")
            return []

    transfer_instructions = parse_transfer_instructions(data)
    trs = find_rows(data)
    appendix_mappings = parse_appendix_mappings(trs)
    
    records = []
    last_title = ''
    
    # Store base functional records temporarily so we can clone them for the appendix
    functional_map = {}
    
    for tr in trs:
        cells = []
        for td in tr.get('children', []):
            text = ''.join(get_text(td)).strip()
            text = re.sub(r'\s+', ' ', text)
            cells.append(text)
        
        if not cells: continue
        
        # Stop parsing main records if we hit the appendix
        if cells == ['Agency', 'Series Title', 'Item Number'] or cells == ['Appendix', 'Agency', 'Series Title', 'Item Number']:
            break

        rc_no = cells[0]
        if nc_config.series_id_pattern.match(rc_no):
            raw_title = cells[1] if len(cells) >= 5 else last_title
            
            # Extract "SEE ALSO" into description instead of dropping it entirely
            see_also = ""
            see_also_match = re.search(r'(?i)\s+(SEE ALSO:.*)', raw_title)
            if see_also_match:
                see_also = see_also_match.group(1).strip()
                clean_title = raw_title[:see_also_match.start()].strip()
            else:
                clean_title = raw_title.strip()

            if len(cells) >= 5:
                last_title = raw_title
                desc = cells[2]
                disp = cells[3]
                cit = cells[4]
            elif len(cells) == 4:
                desc = cells[1]
                disp = cells[2]
                cit = cells[3]
            elif len(cells) == 3:
                desc = cells[1]
                disp = cells[2]
                cit = ''
            else:
                continue

            if see_also:
                desc = f"{desc}\n\n{see_also}".strip()
                
            comments = transfer_instructions.get(rc_no, "")

            raw_record = make_record(
                schema,
                state="nc",
                agency_name="",
                schedule_type="general",
                schedule_id=schedule_id,
                series_id=rc_no,
                series_title=clean_title,
                series_description=desc,
                retention_statement=disp,
                disposition="",
                legal_citation=cit,
                comments=comments,
                last_updated="2025-02-24",
                last_checked=str(date.today()),
                url="https://archives.ncdcr.gov/functional-schedule"
            )
            cleaned = clean_record_fields(raw_record, nc_config)
            records.append(cleaned)
            functional_map[rc_no] = cleaned
  
    for app in appendix_mappings:
        app_record = make_record(
            schema,
            state="nc",
            agency_name=app['agency'],
            schedule_type="specific",
            schedule_id=schedule_id,
            series_id=app['item_number'],
            series_title=app['legacy_title'],
            series_description="",
            retention_statement="SEE FUNCTIONAL SCHEDULE",
            disposition="",
            legal_citation="",
            comments=f"Legacy Item Number mapped to Functional Schedule chapter {schedule_id}.",
            last_updated="2025-02-24",
            last_checked=str(date.today()),
            url="https://www.ncdcr.gov/functional-schedule-state-agencies"
        )
        records.append(clean_record_fields(app_record, nc_config))
            
    return records
