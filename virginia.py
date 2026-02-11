import pdfplumber
import json

def pdf_to_json(input_pdf, output_json):
    data_out = []

    with pdfplumber.open(input_pdf) as pdf:
        for page_index, page in enumerate(pdf.pages):
            # Extract tables from the current page
            tables = page.extract_tables()
            
            for table_index, table in enumerate(tables):
                if not table:
                    continue
                
                # Assuming the first row is the header
                headers = table[0]
                rows = table[1:]
                
                table_data = []
                for row in rows:
                    # Create a dictionary mapping header to cell value
                    entry = dict(zip(headers, row))
                    table_data.append(entry)
                
                data_out.append({
                    "page": page_index + 1,
                    "table_index": table_index,
                    "data": table_data
                })

    # Write the structured data to a JSON file
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(data_out, f, indent=4)

if __name__ == "__main__":
    pdf_to_json("input_data.pdf", "output_data.json")