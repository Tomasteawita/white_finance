import json
import re
import os
import datetime
from pathlib import Path
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

jsonl_path = Path(r"D:\DatosDeMercado\marketing_data\instagram_20260920\processed\chat_corpus_nlp.jsonl")

conversations = {}
for line in open(jsonl_path, "r", encoding="utf-8"):
    try:
        record = json.loads(line)
        cid = record["conversation_id"]
        if cid not in conversations:
            conversations[cid] = []
        conversations[cid].append(record)
    except Exception as e:
        print("Error parsing line", e)

filtered_conversations = {}
for cid, msgs in conversations.items():
    # Sort messages by timestamp
    msgs.sort(key=lambda x: x["timestamp_ms"])
    start_date = msgs[0]["timestamp_iso"].split("T")[0]
    
    if "2026-09-12" <= start_date <= "2026-09-20":
        filtered_conversations[cid] = msgs

print(f"Total conversations in date range: {len(filtered_conversations)}")

# Group by ad URL
ad_url_conversations = {}

for cid, msgs in filtered_conversations.items():
    dialog = []
    ad_url = "Ninguna"
    for msg in msgs:
        full_text = msg.get("full_text", "")
        # Look for ad reply
        match = re.search(r"replied to an ad\. View ad\((https?://.*?)\)", full_text)
        if match:
            ad_url = match.group(1)
        dialog.append(msg.get("full_prompt_context", ""))
    
    full_dialog = "\n".join(dialog)
    if ad_url not in ad_url_conversations:
        ad_url_conversations[ad_url] = []
    
    ad_url_conversations[ad_url].append(full_dialog)

prompt = """
Aquí tienes varias conversaciones de ventas por Instagram, agrupadas por la URL del anuncio del que provinieron.

Por favor, analiza todas las conversaciones y genera un informe estructurado que responda a estas 3 preguntas:
1. ¿Cuales fueron los dolores más mencionados entre todas las conversaciones?
2. ¿Cuales fueron los deseos más mencionados entre todas las conversaciones?
3. A partir del atributo "replied to an ad. View ad(URL)", ¿De todos los dolores y deseos identificados, de qué contenido (URL) vinieron? Detalla explícitamente qué dolores/deseos corresponden a qué URL.

Conversaciones agrupadas por URL:
"""

for url, dialogs in ad_url_conversations.items():
    prompt += f"\n\n--- URL: {url} ---\n"
    for idx, d in enumerate(dialogs):
        prompt += f"\n[Conversación {idx+1}]\n{d}\n"

response = client.models.generate_content(
    model='gemini-3.6-flash',
    contents=prompt
)
with open(r"C:\Users\User\white_finance\reporte_dolores_deseos.md", "w", encoding="utf-8") as f:
    f.write(response.text)
print("Report saved to reporte_dolores_deseos.md")
