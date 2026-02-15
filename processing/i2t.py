from img2table.document import Image, PDF
from img2table.ocr import SuryaOCR
import pandas as pd

# 1. Initialize the OCR engine 
# This will automatically utilize your NVIDIA GPU via the PyTorch dependencies in your Pixi env
ocr = SuryaOCR(langs=["en"])

# 2. Load the document (works for images or PDFs)
# Use 'PDF' for documents or 'Image' for png/jpg
doc = PDF("../input_data.pdf")

# 3. Extract tables
# implicit_rows=True helps catch rows that aren't separated by lines
# borderless_tables=True is essential for modern, clean layouts
extracted_tables = doc.extract_tables(ocr=ocr, 
                                     implicit_rows=True, 
                                     borderless_tables=True)

# 4. Access the Data
# The result is a dictionary: {page_index: [ExtractedTable, ...]}
for page_index, tables in extracted_tables.items():
    for table_idx, table in enumerate(tables):
        # table.df is your ready-to-use Pandas DataFrame
        df = table.df
        print(f"Page {page_index} - Table {table_idx}:")
        print(df.head())