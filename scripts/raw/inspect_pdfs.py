"""Script temporal para inspeccionar la estructura de tablas en los PDFs de cuenta corriente."""
import pdfplumber
import os

pdf_dir = r"c:\Users\tomas\white_finance\data\analytics\cuentas_corrientes"

for pdf_file in os.listdir(pdf_dir):
    if pdf_file.lower().endswith(".pdf"):
        pdf_path = os.path.join(pdf_dir, pdf_file)
        print(f"\n{'='*60}")
        print(f"PDF: {pdf_file}")
        print(f"{'='*60}")
        with pdfplumber.open(pdf_path) as pdf:
            print(f"Número de páginas: {len(pdf.pages)}")
            for i, page in enumerate(pdf.pages):
                print(f"\n--- Página {i+1} ---")
                # Primero intentamos extraer tablas
                tables = page.extract_tables()
                if tables:
                    print(f"  Tablas encontradas: {len(tables)}")
                    for j, table in enumerate(tables):
                        print(f"  Tabla {j+1} ({len(table)} filas):")
                        for k, row in enumerate(table[:5]):  # Mostrar solo las primeras 5 filas
                            print(f"    Fila {k}: {row}")
                        if len(table) > 5:
                            print(f"    ... ({len(table)-5} filas más)")
                else:
                    # Si no hay tablas detectadas, mostramos el texto crudo
                    text = page.extract_text()
                    if text:
                        lines = text.split('\n')
                        print(f"  Sin tablas detectadas. Primeras 10 líneas de texto:")
                        for line in lines[:10]:
                            print(f"    {repr(line)}")
