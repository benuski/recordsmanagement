from img2table.document import PDF
from img2table.ocr import TesseractOCR
import pandas as pd
from pathlib import Path

def merge_multiline_cells(df):
    """Merge rows that are part of the same table row."""
    merged_rows = []
    current_row = None

    for idx, row in df.iterrows():
        first_col = str(row.iloc[0]).strip()

        if first_col and first_col not in ['nan', '']:
            if current_row is not None:
                merged_rows.append(current_row)
            current_row = row.tolist()
        else:
            if current_row is not None:
                for i in range(len(row)):
                    cell_value = str(row.iloc[i]).strip()
                    if cell_value and cell_value not in ['nan', '']:
                        current_row[i] = str(current_row[i]) + ' ' + cell_value

    if current_row is not None:
        merged_rows.append(current_row)

    if merged_rows:
        return pd.DataFrame(merged_rows, columns=df.columns)
    return df

def extract_tables_from_pdf(pdf_path, output_dir="output"):
    """Extract tables using Tesseract OCR."""

    # Initialize Tesseract OCR
    ocr = TesseractOCR(lang="eng", psm=6)  # psm=6 is for uniform block of text

    print(f"Processing: {pdf_path}")
    pdf = PDF(src=pdf_path)

    # Extract tables
    extracted_tables = pdf.extract_tables(
        ocr=ocr,
        implicit_rows=False,
        implicit_columns=False,
        borderless_tables=False,
        min_confidence=50,
    )

    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    pdf_name = Path(pdf_path).stem
    all_dataframes = []
    table_count = 0

    if isinstance(extracted_tables, dict):
        for page_num, page_tables in extracted_tables.items():
            print(f"\n=== Page {page_num} ===")

            if not page_tables:
                print("  No tables found")
                continue

            for idx, table in enumerate(page_tables):
                df = table.df

                print(f"\n--- Raw Table {idx + 1} (before processing) ---")
                print(f"Shape: {df.shape}")
                print(df.head())

                # Set headers from first row
                if len(df) > 0:
                    df.columns = df.iloc[0]
                    df = df[1:].reset_index(drop=True)

                # Merge multi-line cells
                df = merge_multiline_cells(df)

                # Clean up
                df = df.dropna(how='all').dropna(axis=1, how='all')
                df = df.applymap(lambda x: str(x).strip() if pd.notna(x) else x)

                all_dataframes.append(df)

                # Save
                output_file = output_path / f"{pdf_name}_p{page_num}_t{idx + 1}.csv"
                df.to_csv(output_file, index=False)

                print(f"\n--- Processed Table {idx + 1} ---")
                print(f"Shape: {df.shape}")
                print(f"Columns: {list(df.columns)}")
                print(f"Saved: {output_file.name}")
                print(f"\nPreview:\n{df.head(3)}\n")

                table_count += 1

    print(f"\n✓ Extracted {table_count} table(s) total")
    return all_dataframes

if __name__ == "__main__":
    pdf_file = "../input_data.pdf"
    dataframes = extract_tables_from_pdf(pdf_file)
