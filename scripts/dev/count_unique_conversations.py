import json
import os
from pathlib import Path

def count_unique_conversations(file_path: str) -> int:
    """
    Cuenta la cantidad de conversation_id únicos en un archivo JSONL.

    :param file_path: Ruta absoluta o relativa del archivo JSONL.
    :return: Número total de conversation_id únicos.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"El archivo no existe en la ruta: {file_path}")

    unique_conversation_ids = set()
    total_lines = 0

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            total_lines += 1
            line = line.strip()
            if not line:
                continue
            
            try:
                data = json.loads(line)
                # Extraemos el conversation_id si existe en el objeto
                conv_id = data.get("conversation_id")
                if conv_id is not None:
                    unique_conversation_ids.add(conv_id)
            except json.JSONDecodeError as e:
                print(f"Advertencia: Línea {total_lines} no es un JSON válido. Error: {e}")

    print(f"Total de líneas procesadas: {total_lines}")
    print(f"Cantidad de conversation_id únicos: {len(unique_conversation_ids)}")
    return len(unique_conversation_ids)

if __name__ == "__main__":
    target_path = r"D:\DatosDeMercado\marketing_data\instagram_20260802\processed\chat_corpus_nlp.jsonl"
    count_unique_conversations(target_path)
