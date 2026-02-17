import pdfplumber
import json
from pathlib import Path

def extract_tables_to_json(pdf_path, output_path=None):

    """
    Works on pdf's from 2025+
    """
    all_tables = []

    # Open the PDF
    with pdfplumber.open(pdf_path) as pdf:
        # Iterate through each page
        for page_num, page in enumerate(pdf.pages, start=1):
            # Extract tables from the current page
            tables = page.extract_tables()

            # Process each table on the page
            for table_num, table in enumerate(tables, start=1):
                if table:
                    # Convert table to list of dictionaries
                    # Assumes first row contains headers
                    headers = table[0]
                    rows = table[1:]

                    table_data = []
                    for row in rows:
                        # Create dictionary for each row
                        row_dict = {headers[i]: row[i] for i in range(len(headers))}
                        table_data.append(row_dict)

                    # Add metadata and table data
                    all_tables.append({
                        "page": page_num,
                        "table_number": table_num,
                        "data": table_data
                    })

    # Create output dictionary
    result = {
        "source_file": str(pdf_path),
        "total_tables": len(all_tables),
        "tables": all_tables
    }

    # Save to file if output path provided
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

    return result

# Example usage
if __name__ == "__main__":
    # PDF file path
    pdf_file = "GS-102.pdf"

    # Create output filename based on input PDF name
    pdf_path = Path(pdf_file)
    output_file = Path("../../data/va") / f"{pdf_path.stem}.json"

    # Extract and convert
    json_data = extract_tables_to_json(pdf_file, output_file)
