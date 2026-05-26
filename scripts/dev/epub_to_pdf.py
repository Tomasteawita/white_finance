import os
import argparse
import logging
import sys
from typing import Optional

# Configuración de Logging para trazabilidad CNV
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

"""
EPUB TO PDF CONVERTER - SENIOR DATA ENGINEER TOOL
-------------------------------------------------
Este script permite la conversión de archivos EPUB a PDF.
Utiliza la librería 'aspose-words' por su alta fidelidad en la conversión
de documentos complejos, ideal para reportes financieros y literatura técnica.

NOTA PROFESIONAL: 
Para una solución 100% open-source sin marcas de agua, se recomienda instalar 
Pandoc (https://pandoc.org/) y usar la librería 'pypandoc'. 
Este script implementa 'aspose-words' para una ejecución inmediata sin dependencias externas del sistema.

Instalación:
pip install aspose-words
"""

def convert_epub_to_pdf(input_path: str, output_path: Optional[str] = None) -> bool:
    """
    Convierte un archivo EPUB a PDF.
    
    Args:
        input_path: Ruta al archivo .epub
        output_path: Ruta de salida .pdf (opcional)
        
    Returns:
        bool: True si la conversión fue exitosa.
    """
    try:
        import aspose.words as aw
    except ImportError:
        logger.error("La librería 'aspose-words' no está instalada.")
        logger.info("Ejecute: pip install aspose-words")
        return False

    if not os.path.exists(input_path):
        logger.error(f"El archivo no existe: {input_path}")
        return False

    if not input_path.lower().endswith('.epub'):
        logger.error("El archivo de entrada debe ser .epub")
        return False

    if output_path is None:
        output_path = os.path.splitext(input_path)[0] + ".pdf"

    try:
        logger.info(f"Iniciando conversión: {input_path} -> {output_path}")
        
        # Cargar el documento EPUB
        # Aspose.Words detecta automáticamente el formato EPUB y lo carga en su DOM
        doc = aw.Document(input_path)
        
        # Opciones de guardado (se pueden ajustar para optimizar calidad/peso)
        save_options = aw.saving.PdfSaveOptions()
        save_options.compliance = aw.saving.PdfCompliance.PDF17 # Formato estándar robusto
        
        # Guardar como PDF
        doc.save(output_path, save_options)
        
        logger.info(f"Conversión completada con éxito: {output_path}")
        return True

    except Exception as e:
        logger.error(f"Error crítico durante la conversión: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Conversor de EPUB a PDF para Ingeniería de Datos Financieros.")
    parser.add_argument("input", help="Ruta del archivo EPUB o directorio")
    parser.add_argument("-o", "--output", help="Ruta del archivo PDF de salida", default=None)
    
    args = parser.parse_args()

    # Validación de directorio o archivo único
    if os.path.isdir(args.input):
        logger.info(f"Procesando directorio: {args.input}")
        files = [f for f in os.listdir(args.input) if f.lower().endswith('.epub')]
        if not files:
            logger.warning("No se encontraron archivos EPUB en el directorio.")
            return

        for file in files:
            full_path = os.path.join(args.input, file)
            convert_epub_to_pdf(full_path)
    else:
        convert_epub_to_pdf(args.input, args.output)

if __name__ == "__main__":
    main()
