import pdfplumber
import pandas as pd
import os
import re

def clean_cell(cell):
    if pd.isna(cell) or cell is None:
        return ""
    # Convert to string and remove newlines and extra spaces
    return str(cell).replace('\n', '').strip()

def process_pdf(pdf_path, output_csv_path):
    all_rows = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    # Clean each cell in the row
                    cleaned_row = [clean_cell(cell) for cell in row]
                    all_rows.append(cleaned_row)
                    
    if not all_rows:
        print(f"No se encontraron tablas en: {os.path.basename(pdf_path)}")
        return
        
    # The first row is usually the header
    df = pd.DataFrame(all_rows[1:], columns=all_rows[0])
    
    # Save to CSV
    df.to_csv(output_csv_path, index=False, encoding='utf-8')
    print(f"Convertido: {os.path.basename(pdf_path)} -> {os.path.basename(output_csv_path)} ({len(df)} filas)")

def main():
    data_dir = r"c:\Users\tomas\white_finance\data\analytics\cuentas_corrientes"
    
    # Find all PDFs
    for filename in os.listdir(data_dir):
        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(data_dir, filename)
            csv_filename = os.path.splitext(filename)[0] + ".csv"
            csv_path = os.path.join(data_dir, csv_filename)
            
            process_pdf(pdf_path, csv_path)

if __name__ == "__main__":
    main()
