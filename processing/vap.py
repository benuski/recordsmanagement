#!/usr/bin/env python3
"""
Run surya_ocr AND surya_table on a PDF, intersect the data, and output Pandas DataFrames.
Optimized for RTX 4080 Super (16GB VRAM) via Pixi.
"""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
import pandas as pd

def get_surya_env(use_gpu: bool = True) -> dict:
    """Configures environment variables for the 4080 Super."""
    env = os.environ.copy()
    if use_gpu:
        env['TORCH_DEVICE'] = 'cuda'
        env['DETECTOR_BATCH_SIZE'] = '48' 
        env['RECOGNITION_BATCH_SIZE'] = '96'
        env['TABLE_REC_BATCH_SIZE'] = '48' 
    else:
        env['TORCH_DEVICE'] = 'cpu'
        env['DETECTOR_BATCH_SIZE'] = '2'
        env['RECOGNITION_BATCH_SIZE'] = '4'
        env['TABLE_REC_BATCH_SIZE'] = '4'
    return env

def run_surya_command(command: list, env: dict) -> None:
    """Helper to run subprocess commands with proper error handling."""
    print(f"Running command: {' '.join(command)}")
    try:
        subprocess.run(command, check=True, env=env)
    except subprocess.CalledProcessError as e:
        print(f"Error running surya command: {e}")
        sys.exit(1)

def bbox_intersection(text_bbox, cell_bbox):
    """
    Simple intersection check. 
    Returns True if the center of the text_bbox is inside the cell_bbox.
    """
    tx1, ty1, tx2, ty2 = text_bbox
    cx1, cy1, cx2, cy2 = cell_bbox
    
    text_center_x = (tx1 + tx2) / 2
    text_center_y = (ty1 + ty2) / 2
    
    return (cx1 <= text_center_x <= cx2) and (cy1 <= text_center_y <= cy2)

def get_bbox_from_item(item):
    """
    Handles variations where row/col items are either:
    1. A list: [x1, y1, x2, y2]
    2. A dict: {'bbox': [x1, y1, x2, y2], ...}
    """
    if isinstance(item, dict):
        return item.get('bbox', [])
    return item

def parse_to_dataframe(ocr_data: dict, table_data: dict, page_name: str) -> list[pd.DataFrame]:
    """
    Combines OCR text and Table layout to build DataFrames.
    Robustly handles dictionary-based rows/cols and missing table bboxes.
    """
    dfs = []
    
    # Handle key matching (filenames vs paths)
    page_ocr = ocr_data.get(page_name)
    page_tables = table_data.get(page_name)

    if page_ocr is None:
        for k in ocr_data.keys():
            if page_name in k or k in page_name:
                page_ocr = ocr_data[k]
                break
    
    if page_tables is None:
        for k in table_data.keys():
            if page_name in k or k in page_name:
                page_tables = table_data[k]
                break

    if not page_tables or not page_ocr:
        return []

    # Iterate through each table detected on the page
    for table in page_tables:
        raw_rows = table.get('rows', [])
        raw_cols = table.get('cols', [])
        
        if not raw_rows or not raw_cols:
            continue

        # Normalize rows/cols to pure lists of [x1, y1, x2, y2]
        rows = [get_bbox_from_item(r) for r in raw_rows]
        cols = [get_bbox_from_item(c) for c in raw_cols]

        # --- Calculate bbox if missing ---
        if 'bbox' in table:
            table_bbox = table['bbox']
        else:
            try:
                # rows/cols are now guaranteed to be lists [x1, y1, x2, y2]
                x1 = min(c[0] for c in cols)
                y1 = min(r[1] for r in rows)
                x2 = max(c[2] for c in cols)
                y2 = max(r[3] for r in rows)
                table_bbox = [x1, y1, x2, y2]
            except (ValueError, IndexError):
                continue

        # Initialize empty grid
        grid = [[[] for _ in range(len(cols))] for _ in range(len(rows))]

        # Flatten OCR lines
        text_lines = []
        for text_block in page_ocr:
             if 'bbox' in text_block and 'text' in text_block:
                 text_lines.append(text_block)
             elif 'text_lines' in text_block:
                 text_lines.extend(text_block['text_lines'])

        for line in text_lines:
            if 'bbox' not in line: continue
            
            t_bbox = line['bbox']
            
            # Optimization: Skip text outside the table generally
            if not bbox_intersection(t_bbox, table_bbox):
                continue

            # Find specific cell match
            matched = False
            for r_idx, row_bbox in enumerate(rows):
                if matched: break
                for c_idx, col_bbox in enumerate(cols):
                    # Construct a virtual cell box
                    cell_box = [col_bbox[0], row_bbox[1], col_bbox[2], row_bbox[3]]
                    
                    if bbox_intersection(t_bbox, cell_box):
                        grid[r_idx][c_idx].append(line['text'])
                        matched = True
                        break
        
        # Convert lists of strings to single strings and create DF
        clean_grid = []
        for r in grid:
            clean_row = [" ".join(cell).strip() for cell in r]
            clean_grid.append(clean_row)

        df = pd.DataFrame(clean_grid)
        dfs.append(df)
        
    return dfs

def main():
    parser = argparse.ArgumentParser(
        description="Extract tables from PDF to Pandas using surya_ocr and surya_table"
    )
    parser.add_argument("pdf_file", type=str, help="Path to the PDF file")
    parser.add_argument("--output_dir", type=str, default="ocr_output", 
                       help="Directory to save OCR results")
    parser.add_argument("--cpu", action="store_true", help="Force CPU usage")
    parser.add_argument("--skip_processing", action="store_true", 
                       help="Skip surya execution and only re-process JSONs")

    args = parser.parse_args()
    
    pdf_path = Path(args.pdf_file)
    output_dir = Path(args.output_dir)
    pdf_name = pdf_path.stem
    
    pdf_output_dir = output_dir / pdf_name
    pdf_output_dir.mkdir(parents=True, exist_ok=True)
    
    env = get_surya_env(use_gpu=not args.cpu)

    # Separate output directories for text and layout
    ocr_subdir = output_dir / f"{pdf_name}_text"
    table_subdir = output_dir / f"{pdf_name}_layout"
    
    if not args.skip_processing:
        print("--- Step 1: Running Text Recognition ---")
        run_surya_command(["surya_ocr", str(pdf_path), "--output_dir", str(ocr_subdir)], env)

        print("\n--- Step 2: Running Table Structure Recognition ---")
        run_surya_command(["surya_table", str(pdf_path), "--output_dir", str(table_subdir)], env)

    try:
        # Load JSONs
        ocr_file = ocr_subdir / pdf_name / "results.json"
        table_file = table_subdir / pdf_name / "results.json"
        
        print(f"Loading OCR from: {ocr_file}")
        with open(ocr_file, 'r') as f:
            ocr_data = json.load(f)
            
        print(f"Loading Tables from: {table_file}")
        with open(table_file, 'r') as f:
            table_data = json.load(f)
            
    except FileNotFoundError as e:
        print(f"Error loading JSON results: {e}")
        return 1

    print("\n--- Step 3: Constructing DataFrames ---")
    
    # Iterate through keys
    for page in table_data.keys():
        print(f"\nProcessing {page}...")
        dfs = parse_to_dataframe(ocr_data, table_data, page)
        
        if not dfs:
            print("No tables extracted for this page.")
            
        for i, df in enumerate(dfs):
            print(f"\nTable {i+1} found:")
            print(df.to_markdown(index=False)) 
            
            # Save to CSV
            csv_name = f"{page}_table_{i+1}.csv"
            safe_name = "".join([c for c in csv_name if c.isalpha() or c.isdigit() or c in '._-'])
            
            out_path = pdf_output_dir / safe_name
            df.to_csv(out_path, index=False)
            print(f"Saved to {out_path}")

if __name__ == "__main__":
    main()
