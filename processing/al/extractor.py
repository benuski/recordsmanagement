import re
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import date, datetime

from processing.base_config import StateScheduleConfig
from processing.central_file import make_record, clean_record_fields

logger = logging.getLogger(__name__)

class AlabamaNarrativeExtractor:
    # Standardized Trigger Codes
    # AC: After Closed
    # AV: As Long as Administratively Valuable
    # CE: Calendar Year End
    # FE: Fiscal Year End
    # LA: Life of Asset
    # PM: Permanent
    # US: Until Superseded
    # CR: Creation
    
    def __init__(self, config: StateScheduleConfig, schema: Dict[str, Any]):
        self.config = config
        self.schema = schema
        self.agency_name = ""
        self.effective_date = None
        self.records = {} # Title -> {description, disposition_full, subfunction, citation, trigger, duration}
        self.bib_map = {} # Bibliographic Title -> Main Title

    def parse_markdown(self, md_content: str, schedule_id: str):
        lines = md_content.split('\n')
        
        # 1. Extract Agency Name and Effective Date
        self._extract_metadata(lines[:50])

        # 2. Identify major sections
        appraisal_start = -1
        requirements_start = -1
        
        for i, line in enumerate(lines):
            if "Records Appraisal" in line:
                if appraisal_start == -1:
                    appraisal_start = i
            if "Records Disposition Requirements" in line:
                requirements_start = i

        if appraisal_start != -1:
            appraisal_end = requirements_start if requirements_start != -1 else len(lines)
            self._parse_appraisal(lines[appraisal_start:appraisal_end])

        if requirements_start != -1:
            self._parse_requirements(lines[requirements_start:])

        return self._finalize_records(schedule_id)

    def _extract_metadata(self, lines: List[str]):
        # Agency name is usually the first bold line
        for line in lines[:20]:
            clean_line = line.strip().strip('*').strip('#').strip()
            if clean_line and not self.agency_name:
                self.agency_name = clean_line
                break
        
        # Effective date usually follows "State Records Commission" in the header
        found_header = False
        for line in lines:
            line_strip = line.strip().strip('*').strip()
            if "State Records Commission" in line_strip:
                found_header = True
                continue
            
            if found_header and line_strip:
                # Try to parse date
                # Formats: "October 21, 2015" or "April 26, 2002"
                date_match = re.search(r'([A-Z][a-z]+\s+\d{1,2},\s+\d{4})', line_strip)
                if date_match:
                    try:
                        dt = datetime.strptime(date_match.group(1), '%B %d, %Y')
                        self.effective_date = dt.strftime('%Y-%m-%d')
                        break
                    except ValueError:
                        pass
                # Don't keep looking forever if we see Table of Contents
                if "Table of Contents" in line_strip:
                    break

    def _parse_appraisal(self, lines: List[str]):
        current_subfunction = ""
        current_title = ""
        current_desc = []
        found_any_record = False

        def save_current():
            nonlocal current_title, current_desc, found_any_record
            if not current_title: return
            
            found_any_record = True
            desc_text = " ".join(current_desc).strip()
            
            bib_match = re.search(r'\(Bibliographic Title:\s*([^)]+)\)', desc_text)
            bib_title = bib_match.group(1).strip() if bib_match else None
            
            clean_desc = re.sub(r'\**\(Bibliographic Title:[^)]+\)\**', '', desc_text).strip()
            
            citation = None
            if self.config.legal_citation_pattern:
                cit_match = self.config.legal_citation_pattern.search(clean_desc)
                if cit_match:
                    citation = cit_match.group(0)

            title_key = current_title.upper()
            if title_key not in self.records:
                self.records[title_key] = {}
            
            self.records[title_key].update({
                'description': clean_desc,
                'subfunction': current_subfunction,
                'citation': citation
            })
            if bib_title:
                self.bib_map[bib_title.upper()] = title_key
            
            current_title = ""
            current_desc = []

        for line in lines:
            line_strip = line.strip()
            if not line_strip: continue

            if (line_strip.startswith('# ') or line_strip.startswith('## ')):
                if "Records Appraisal" not in line_strip:
                    save_current()
                    return

            if line_strip.startswith('###') or (line_strip.startswith('**') and line_strip.endswith('**') and len(line_strip) < 100):
                new_sub = line_strip.strip('#').strip('*').strip()
                if "TEMPORARY RECORDS" not in new_sub.upper() and "PERMANENT RECORDS" not in new_sub.upper():
                    current_subfunction = new_sub
                continue

            title_match = re.match(r'^(?:- )?\*\*([^*.]+?)\.?\*\*\.?\s*(.*)', line_strip)
            if title_match:
                save_current()
                current_title = title_match.group(1).strip()
                current_desc = [title_match.group(2).strip()]
            elif current_title:
                current_desc.append(line_strip)

        save_current()

    def _parse_requirements(self, lines: List[str]):
        current_subfunction = ""
        parent_title = ""
        current_title = ""
        current_disp_lines = []
        
        def save_req():
            nonlocal current_title, current_disp_lines
            if not current_title or not current_disp_lines: return
            
            disp_full = " ".join(current_disp_lines).strip()
            title_key = current_title.upper()
            
            lookup_key = None
            if title_key in self.bib_map:
                lookup_key = self.bib_map[title_key]
            elif title_key in self.records:
                lookup_key = title_key
            elif parent_title and parent_title.upper() in self.records:
                lookup_key = parent_title.upper()
            
            if title_key not in self.records:
                self.records[title_key] = {}
                
            if lookup_key and lookup_key in self.records:
                parent_data = self.records[lookup_key]
                self.records[title_key].setdefault('description', parent_data.get('description', ''))
                self.records[title_key].setdefault('citation', parent_data.get('citation', ''))
                self.records[title_key].setdefault('subfunction', current_subfunction or parent_data.get('subfunction', ''))
            
            self.records[title_key]['subfunction'] = current_subfunction or self.records[title_key].get('subfunction', '')
            self.records[title_key]['disposition_full'] = disp_full
            trigger, duration = self._standardize_retention(disp_full)
            self.records[title_key]['trigger'] = trigger
            self.records[title_key]['duration'] = duration
            
            if current_title != parent_title:
                current_title = ""
            current_disp_lines = []

        for line in lines:
            line_strip = line.strip()
            if not line_strip: continue

            if line_strip.startswith('###'):
                save_req()
                current_subfunction = line_strip.strip('#').strip()
                parent_title = ""
                current_title = ""
                continue

            if (line_strip.isupper() and len(line_strip) > 3) or (line_strip.startswith('**') and line_strip.endswith('**')):
                save_req()
                parent_title = line_strip.strip('*').strip()
                current_title = parent_title
                continue

            sub_item_match = re.match(r'^([a-z\d]\.)\s*(.*)', line_strip)
            if sub_item_match:
                save_req()
                sub_title = sub_item_match.group(2).strip()
                if sub_title:
                    current_title = f"{parent_title} - {sub_title}" if parent_title else sub_title
                continue

            if "Disposition:" in line_strip:
                save_req()
                disp_parts = line_strip.split("Disposition:", 1)
                if not current_title and disp_parts[0].strip():
                    current_title = disp_parts[0].strip().strip('*').strip()
                current_disp_lines = [disp_parts[1].strip()]
            elif current_disp_lines:
                current_disp_lines.append(line_strip)

        save_req()

    def _standardize_retention(self, text: str) -> tuple[str, Optional[int]]:
        text_lower = text.lower()
        if "permanent" in text_lower:
            return "PM", 999
        if "useful life" in text_lower:
            return "AV", None
        if "superseded" in text_lower:
            return "US", None

        year_match = re.search(r'(\d+)\s*years?', text_lower)
        duration = int(year_match.group(1)) if year_match else None
        
        if "after" in text_lower:
            return "AC", duration
        if "fiscal year" in text_lower:
            return "FE", duration
        if "calendar year" in text_lower:
            return "CE", duration

        if duration is not None:
            return "CR", duration
        return "AV", None

    def _finalize_records(self, schedule_id: str) -> List[Dict[str, Any]]:
        processed = []
        for title, data in self.records.items():
            if 'trigger' not in data: continue 
            
            raw_record = make_record(
                self.schema,
                state="al",
                schedule_type="specific",
                schedule_id=schedule_id,
                series_id="", 
                series_title=title,
                series_description=data.get('description', ''),
                trigger_event=data.get('trigger', ''),
                duration_years=data.get('duration'),
                disposition=data.get('disposition_full', ''),
                legal_citation=data.get('citation', ''),
                agency_name=self.agency_name,
                last_updated=self.effective_date,
                last_checked=str(date.today())
            )
            processed_rec = clean_record_fields(raw_record, self.config)
            processed_rec['retention_rules']['trigger_event'] = data.get('trigger', '')
            processed_rec['retention_rules']['duration_years'] = data.get('duration')
            
            processed.append(processed_rec)
        return processed

def parse_alabama_docx(docx_path: Path, schedule_id: str, schema: Dict[str, Any], config: StateScheduleConfig) -> List[Dict[str, Any]]:
    md_path = docx_path.with_suffix('.md')
    try:
        import subprocess
        if not md_path.exists() or docx_path.stat().st_mtime > md_path.stat().st_mtime:
            subprocess.run(["pandoc", "-f", "docx", "-t", "markdown", str(docx_path), "-o", str(md_path)], check=True)
        with open(md_path, 'r', encoding='utf-8') as f:
            md_content = f.read()
        extractor = AlabamaNarrativeExtractor(config, schema)
        return extractor.parse_markdown(md_content, schedule_id)
    except Exception as e:
        logger.error(f"Failed to parse Alabama docx {docx_path}: {e}")
        return []
