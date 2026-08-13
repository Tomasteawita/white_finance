import pandas as pd
from pathlib import Path

# Cargar datasets
main_df = pd.read_csv('D:/DatosDeMercado/marketing_data/instagram_20260802/processed/instagram_audio_transcripts.csv', encoding='latin-1')
segs_df = pd.read_csv('D:/DatosDeMercado/marketing_data/instagram_20260802/processed/seguimientos_por_conversacion.csv')

# Ordenar las conversaciones
main_df = main_df.sort_values(['conversation_id', 'timestamp_ms'])

TOMAS_PATTERN = 'Tomas Santiago Cueva'

respuestas = []

for _, seg_row in segs_df.iterrows():
    conv_id = seg_row['conversation_id']
    texto_seg = seg_row['followup_text']
    
    # Encontrar la fila en main_df correspondiente al seguimiento
    conv_df = main_df[main_df['conversation_id'] == conv_id].reset_index()
    
    # Hay un tema, el followup_text puede tener varios mensajes unidos con ' | '
    # Buscamos el ultimo mensaje del seguimiento
    last_msg_texto = str(texto_seg).split(' | ')[-1]
    
    # Buscamos el índice en conv_df donde Tomas dice eso
    idx_seg = None
    for idx, row in conv_df.iterrows():
        # clean and match
        if TOMAS_PATTERN in str(row['sender_name']) and last_msg_texto[:100] in str(row['full_text']):
            idx_seg = idx
            
    # Si encontramos el mensaje, miramos el siguiente
    tuvo_respuesta = False
    if idx_seg is not None and idx_seg + 1 < len(conv_df):
        next_row = conv_df.iloc[idx_seg + 1]
        # Si el siguiente no es Tomas, significa que el cliente respondio
        if TOMAS_PATTERN not in str(next_row['sender_name']):
            tuvo_respuesta = True
            
    respuestas.append(tuvo_respuesta)

from google import genai
import sys
sys.path.append(r'C:\Users\tomas\white_finance\scripts\layers\marketing\analisis de conversaciones')
from analisis_seguimientos_flujo import normalizar_estrategias, GEMINI_API_KEY

# Normalizar a estrategias canónicas
unicas = segs_df['estrategia'].dropna().unique().tolist()
client = genai.Client(api_key=GEMINI_API_KEY)

print("Normalizando estrategias con Gemini...")
mapeo = normalizar_estrategias(unicas, client)
segs_df['estrategia_canonica'] = segs_df['estrategia'].map(mapeo).fillna(segs_df['estrategia'])

segs_df['tuvo_respuesta'] = respuestas

# Reporte por estrategia canónica
reporte = segs_df.groupby('estrategia_canonica').agg(
    total=('id', 'count'),
    respuestas=('tuvo_respuesta', 'sum')
).reset_index()

reporte['tasa_respuesta_%'] = (reporte['respuestas'] / reporte['total'] * 100).round(1)
reporte = reporte.sort_values('tasa_respuesta_%', ascending=False)

print("="*60)
print("TASA DE RESPUESTA POR ESTRATEGIA DE SEGUIMIENTO")
print("="*60)
print(reporte.to_string(index=False))

# Guardar
segs_df.to_csv('D:/DatosDeMercado/marketing_data/instagram_20260802/processed/seguimientos_por_conversacion_con_respuesta.csv', index=False)
