from img2table.document import PDF
from img2table.ocr import SuryaOCR
import pandas as pd

def pdf_to_dataframe(pdf_path, output_csv=None, languages=None):
    """
    Extract tables from a PDF using img2table with surya-ocr backend.

    Args:
        pdf_path (str): Path to the PDF file
        output_csv (str, optional): Path to save the first table as CSV
        languages (list, optional): List of language codes (default: ["en"])

    Returns:
        list: List of pandas DataFrames, one for each table found
    """
    # Set default language if not provided
    if languages is None:
        languages = ["en"]

    # Initialize Surya OCR backend with languages
    ocr = SuryaOCR(langs=languages)

    # Load the PDF document
    pdf = PDF(src=pdf_path)

    # Extract tables from the PDF
    extracted_tables = pdf.extract_tables(
        ocr=ocr,
        implicit_rows=True,
        borderless_tables=True,
        min_confidence=50
    )

    # Convert extracted tables to DataFrames
    dataframes = []

    for page_num, page_tables in extracted_tables.items():
        print(f"\nPage {page_num + 1}: Found {len(page_tables)} table(s)")

        for idx, table in enumerate(page_tables):
            # Convert table to DataFrame
            df = table.df
            dataframes.append(df)

            print(f"  Table {idx + 1}: {df.shape[0]} rows × {df.shape[1]} columns")
            print(df.head())

    # Optionally save the first table to CSV
    if output_csv and dataframes:
        dataframes[0].to_csv(output_csv, index=False)
        print(f"\nFirst table saved to: {output_csv}")

    return dataframes

# Example usage
if __name__ == "__main__":
    pdf_file = "../input_data.pdf"

    # Extract all tables (English language)
    tables = pdf_to_dataframe(pdf_file, output_csv="output_table.csv")

    # For multiple languages, use:
    # tables = pdf_to_dataframe(pdf_file, languages=["en", "es", "fr"])

    # Access individual tables
    if tables:
        print(f"\nTotal tables extracted: {len(tables)}")

        # Work with the first table
        first_table = tables[0]
        print("\nFirst table details:")
        print(first_table.info())
