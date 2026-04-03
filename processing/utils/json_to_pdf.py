#!/usr/bin/env python3
import json
import argparse
import subprocess
import tempfile
import sys
from pathlib import Path

def generate_markdown(data):
    lines = []
    
    # Ensure data is a list
    if not isinstance(data, list):
        data = [data]
        
    if not data:
        return "# No records found\n"
        
    first_record = data[0]
    metadata = first_record.get("schedule_metadata", {})
    agency_name = metadata.get("agency_name", "Unknown Agency")
    schedule_id = metadata.get("schedule_id", "Unknown Schedule")
    state = metadata.get("state", "VA").upper()
    
    lines.append(f"# Retention Schedule: {agency_name} ({state} - {schedule_id})")
    lines.append("")
    
    for item in data:
        series_meta = item.get("series_metadata", {})
        rules = item.get("retention_rules", {})
        
        series_id = series_meta.get("series_id", "N/A")
        title = series_meta.get("series_title", "Untitled")
        desc = series_meta.get("series_description", "No description.")
        
        lines.append(f"## {series_id}: {title}")
        lines.append(f"**Description:** {desc}")
        lines.append("")
        
        trigger = rules.get("trigger_desc", "N/A")
        duration_y = rules.get("duration_years")
        duration_m = rules.get("duration_months")
        disp = rules.get("disposition_method", "N/A")
        confidential = rules.get("confidential_flag", False)
        
        retention = trigger
        if duration_y is not None and duration_y != 999:
            retention += f" ({duration_y} years"
            if duration_m is not None:
                retention += f", {duration_m} months"
            retention += ")"
        elif duration_y == 999:
            retention += " (Permanent)"
            
        lines.append(f"- **Retention:** {retention}")
        lines.append(f"- **Disposition:** {disp}")
        lines.append(f"- **Confidential:** {'Yes' if confidential else 'No'}")
        lines.append("")
        
    return "\n".join(lines)

def main():
    parser = argparse.ArgumentParser(description="Convert a VA retention schedule JSON file to PDF/A using Pandoc and ConTeXt.")
    parser.add_argument("input_json", type=Path, help="Path to the input JSON file (e.g., data/va/100-001.json)")
    parser.add_argument("output_pdf", type=Path, help="Path for the output PDF file")
    
    args = parser.parse_args()
    
    if not args.input_json.is_file():
        print(f"Error: Input file {args.input_json} does not exist.", file=sys.stderr)
        sys.exit(1)
        
    with open(args.input_json, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}", file=sys.stderr)
            sys.exit(1)
            
    if not data:
        print("Error: No data found in JSON file.", file=sys.stderr)
        sys.exit(1)
        
    first_record = data[0]
    metadata = first_record.get("schedule_metadata", {})
    agency_name = metadata.get("agency_name", "Retention Schedule")
    schedule_id = metadata.get("schedule_id", "")
    
    markdown_content = generate_markdown(data)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.md', encoding='utf-8', delete=False) as temp_md:
        temp_md.write(markdown_content)
        temp_md_path = Path(temp_md.name)
        
    print(f"Generated temporary markdown at {temp_md_path}")
    
    template_path = Path(__file__).parent.parent / "resources" / "template.tex"
    
    cmd = [
        "pixi", "run", "pandoc",
        str(temp_md_path),
        "-o",
        str(args.output_pdf),
        "--pdf-engine=context",
        "-V", "pdfa=4u",
        "-V", f"agency={agency_name}",
        "-V", f"id={schedule_id}"
    ]
    
    if template_path.exists():
        cmd.extend(["--template", str(template_path)])
        
    print(f"Running command: {' '.join(cmd)}")
    
    try:
        subprocess.run(cmd, check=True)
        print(f"Successfully generated PDF/A at {args.output_pdf}")
    except subprocess.CalledProcessError as e:
        print(f"Pandoc failed with exit code {e.returncode}.", file=sys.stderr)
        sys.exit(e.returncode)
    except FileNotFoundError:
        print("Error: 'pandoc' command not found. Please ensure Pandoc and ConTeXt are installed and in your PATH.", file=sys.stderr)
        sys.exit(1)
    finally:
        if temp_md_path.exists():
            temp_md_path.unlink()

if __name__ == "__main__":
    main()
