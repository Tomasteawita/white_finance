import pandas as pd

df = pd.read_csv('D:/DatosDeMercado/marketing_data/instagram_20260802/processed/instagram_audio_transcripts.csv', encoding='latin-1')

# Cliente = no Tomas Cueva
df_cliente = df[~df['sender_name'].str.contains('Cueva', na=False)].copy()
df_cliente_sorted = df_cliente.sort_values(['conversation_id', 'timestamp_ms'])
primer_mensaje_cliente = df_cliente_sorted.groupby('conversation_id').first().reset_index()

# Filtrar FLUJO
flujo_ids = primer_mensaje_cliente[
    primer_mensaje_cliente['full_text'].str.strip().str.upper().str.startswith('FLUJO', na=False)
]['conversation_id'].tolist()

df_flujo = df[df['conversation_id'].isin(flujo_ids)].copy()
df_cliente_flujo = df_flujo[~df_flujo['sender_name'].str.contains('Cueva', na=False)]
msgs_por_conv = df_cliente_flujo.groupby('conversation_id')['full_text'].count().sort_values(ascending=False)

print('Top conversaciones por mensajes de cliente:')
print(msgs_por_conv.head(10))

# Ver las 3 mas largas
for top_id in msgs_por_conv.index[:3]:
    print(f'\n{"="*60}')
    print(f'Conversacion: {top_id}')
    print('='*60)
    sample = df_flujo[df_flujo['conversation_id'] == top_id].sort_values('timestamp_ms')
    for _, row in sample.iterrows():
        sender = 'TOMAS' if 'Cueva' in str(row['sender_name']) else 'CLIENTE'
        texto = str(row['full_text'])[:300]
        print(f'[{sender}] {texto}')
