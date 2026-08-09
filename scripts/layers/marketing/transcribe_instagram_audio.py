"""
Script de Procesamiento y Transcripción de Audios de Instagram para PLN / NLP.

Este módulo recorre las conversaciones exportadas de Instagram en formato JSON (`inbox`),
identifica notas de voz/archivos de audio, los transcribe utilizando el modelo Whisper
y genera archivos consolidados en JSONL y Excel estructurados para análisis en Lenguaje Natural.

Uso:
    python transcribe_instagram_audio.py --inbox_path "D:\DatosDeMercado\marketing_data\instagram_20260802\your_instagram_activity\messages\inbox" --output_dir "D:\DatosDeMercado\marketing_data\instagram_20260802\processed"
"""

import os
import json
import glob
import argparse
import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd

import sys

# Forzar encoding UTF-8 en stdout/stderr para la consola de Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# Intentar importar ftfy para limpiar mojibake de Meta; si no está, usar fallback
try:
    import ftfy
    def fix_encoding(text: str) -> str:
        return ftfy.fix_text(text)
except ImportError:
    def fix_encoding(text: str) -> str:
        if not isinstance(text, str):
            return text
        try:
            return text.encode('latin1').decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError):
            return text

# Intentar importar faster_whisper o whisper
WHISPER_BACKEND = None
try:
    from faster_whisper import WhisperModel
    WHISPER_BACKEND = "faster_whisper"
except ImportError:
    try:
        import whisper
        WHISPER_BACKEND = "openai_whisper"
    except ImportError:
        WHISPER_BACKEND = None


def load_whisper_model(model_size: str = "base", device: str = "cpu"):
    """
    Carga el modelo Whisper según las librerías disponibles.
    
    Args:
        model_size: Tamaño del modelo ('tiny', 'base', 'small', 'medium', 'large-v3').
        device: Dispositivo de cómputo ('cpu' o 'cuda').
    
    Returns:
        Instancia cargada del modelo Whisper.
    """
    if WHISPER_BACKEND == "faster_whisper":
        print(f"[INFO] Cargando modelo faster-whisper '{model_size}' en {device}...")
        return WhisperModel(model_size, device=device, compute_type="int8")
    elif WHISPER_BACKEND == "openai_whisper":
        print(f"[INFO] Cargando modelo openai-whisper '{model_size}' en {device}...")
        return whisper.load_model(model_size, device=device)
    else:
        print("[WARNING] Ni 'faster-whisper' ni 'openai-whisper' están disponibles. Las transcripciones no se ejecutarán.")
        return None


def transcribe_audio_file(audio_path: str, model: Any) -> Dict[str, Any]:
    """
    Transcribe un archivo de audio utilizando el modelo Whisper cargado.
    
    Args:
        audio_path: Ruta al archivo de audio (.m4a, .mp4, .ogg, etc.)
        model: Objeto de modelo Whisper cargado.
        
    Returns:
        Dict con el texto transcrito, idioma detectado y estado de la operación.
    """
    if not os.path.exists(audio_path):
        return {
            "transcript_text": "",
            "language": None,
            "status": "file_not_found"
        }
    
    if model is None:
        return {
            "transcript_text": "[STT No Disponible]",
            "language": None,
            "status": "model_missing"
        }
        
    try:
        if WHISPER_BACKEND == "faster_whisper":
            segments, info = model.transcribe(audio_path, beam_size=5)
            text = " ".join([segment.text.strip() for segment in segments])
            return {
                "transcript_text": text,
                "language": info.language,
                "status": "success"
            }
        elif WHISPER_BACKEND == "openai_whisper":
            result = model.transcribe(audio_path)
            return {
                "transcript_text": result.get("text", "").strip(),
                "language": result.get("language"),
                "status": "success"
            }
    except Exception as e:
        print(f"[ERROR] Falló la transcripción de {audio_path}: {e}")
        return {
            "transcript_text": "",
            "language": None,
            "status": f"error: {str(e)}"
        }

    return {"transcript_text": "", "language": None, "status": "unknown_error"}


def parse_instagram_inbox(inbox_path: str, model: Any, base_search_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Recorre iterativamente las subcarpetas del inbox de Instagram y procesa todos los archivos message_*.json.
    
    Args:
        inbox_path: Ruta al directorio inbox.
        model: Modelo de transcripción Whisper.
        base_search_dir: Directorio base de la exportación para resolver rutas relativas de audio.
        
    Returns:
        Lista de registros estructurados para PLN.
    """
    inbox_dir = Path(inbox_path)
    if not inbox_dir.exists():
        raise FileNotFoundError(f"No se encontró el directorio inbox en: {inbox_path}")
        
    if base_search_dir is None:
        # Asumir que la raíz de exportación está 3 niveles arriba de inbox si sigue la convención estándar
        base_search_dir = str(inbox_dir.parent.parent.parent)

    corpus_records = []
    message_files = glob.glob(str(inbox_dir / "**" / "message_*.json"), recursive=True)
    print(f"[INFO] Se encontraron {len(message_files)} archivos de mensajes JSON.")

    for json_file in message_files:
        thread_folder = Path(json_file).parent.name
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"[WARNING] No se pudo leer {json_file}: {e}")
            continue

        participants = [fix_encoding(p.get("name", "")) for p in data.get("participants", [])]
        messages = data.get("messages", [])

        # Procesar mensajes en orden cronológico (los JSONs de Instagram suelen venir de más reciente a más antiguo)
        for msg in reversed(messages):
            sender_name = fix_encoding(msg.get("sender_name", "Desconocido"))
            timestamp_ms = msg.get("timestamp_ms", 0)
            dt_iso = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0).isoformat() if timestamp_ms else ""
            content = fix_encoding(msg.get("content", ""))
            
            # Detectar notas de voz / audios
            audio_files = msg.get("audio_files", [])
            photos = msg.get("photos", [])
            videos = msg.get("videos", [])
            
            is_audio = len(audio_files) > 0
            transcript_info = {"transcript_text": "", "language": None, "status": "no_audio"}
            relative_audio_path = None
            
            if is_audio:
                audio_item = audio_files[0]
                relative_audio_path = audio_item.get("uri", "")
                # Construir la ruta absoluta intentando varias opciones de resolución
                possible_paths = [
                    Path(base_search_dir) / relative_audio_path,
                    inbox_dir.parent / relative_audio_path,
                    Path(json_file).parent / relative_audio_path,
                    Path(relative_audio_path)
                ]
                
                audio_abs_path = None
                for p in possible_paths:
                    if p.exists():
                        audio_abs_path = str(p)
                        break

                if audio_abs_path:
                    print(f"[INFO] Transcribiendo nota de voz de {sender_name}: {audio_abs_path}")
                    transcript_info = transcribe_audio_file(audio_abs_path, model)
                else:
                    print(f"[WARNING] Archivo de audio no encontrado localmente: {relative_audio_path}")
                    transcript_info["status"] = "file_not_found"

            # Formatear el texto completo para procesamiento en PNL / Prompt de LLM
            final_text_content = content
            if is_audio and transcript_info.get("transcript_text"):
                audio_text = transcript_info["transcript_text"]
                final_text_content = f"[AUDIO TRANSCRITO]: {audio_text}" if not content else f"{content} | [AUDIO TRANSCRITO]: {audio_text}"

            prompt_context_line = f"[{dt_iso}] {sender_name}: {final_text_content}".strip()

            record = {
                "conversation_id": thread_folder,
                "participants": participants,
                "sender_name": sender_name,
                "timestamp_ms": timestamp_ms,
                "timestamp_iso": dt_iso,
                "message_type": "audio" if is_audio else ("text" if content else "other_media"),
                "content_raw": content,
                "audio_relative_path": relative_audio_path,
                "transcript_text": transcript_info.get("transcript_text", ""),
                "transcript_status": transcript_info.get("status"),
                "language_detected": transcript_info.get("language"),
                "full_text": final_text_content,
                "full_prompt_context": prompt_context_line
            }
            corpus_records.append(record)

    return corpus_records


def save_nlp_outputs(records: List[Dict[str, Any]], output_dir: str):
    """
    Guarda los registros estructurados en formatos JSONL, JSON y Excel para análisis PLN.
    
    Args:
        records: Lista de diccionarios procesados.
        output_dir: Directorio de destino.
    """
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # 1. Guardar en JSONL (Formato canónico para modelos de lenguaje / NLP)
    jsonl_file = out_path / "chat_corpus_nlp.jsonl"
    with open(jsonl_file, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"[SUCCESS] Corpus NLP guardado en: {jsonl_file}")

    # 2. Guardar en JSON estructurado completo
    json_file = out_path / "chat_corpus_nlp.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"[SUCCESS] JSON completo guardado en: {json_file}")

    # 3. Guardar en Excel para exploración tabular
    df = pd.DataFrame(records)
    excel_file = out_path / "instagram_audio_transcripts.xlsx"
    df.to_excel(excel_file, index=False)
    print(f"[SUCCESS] Excel navegable guardado en: {excel_file}")


def main():
    parser = argparse.ArgumentParser(description="Procesador y Transcriptor de Audios de Instagram para PLN.")
    parser.add_argument("--inbox_path", type=str, required=True, help="Ruta al directorio 'inbox' de Instagram.")
    parser.add_argument("--output_dir", type=str, required=True, help="Directorio donde guardar los resultados procesados.")
    parser.add_argument("--model_size", type=str, default="base", help="Tamaño del modelo Whisper (tiny, base, small, medium, large-v3).")
    parser.add_argument("--device", type=str, default="cpu", help="Dispositivo de ejecución ('cpu' o 'cuda').")
    
    args = parser.parse_args()

    # Cargar modelo Whisper
    model = load_whisper_model(model_size=args.model_size, device=args.device)

    # Procesar conversaciones
    records = parse_instagram_inbox(args.inbox_path, model)

    # Exportar datasets
    save_nlp_outputs(records, args.output_dir)


if __name__ == "__main__":
    main()
