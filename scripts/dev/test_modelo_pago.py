"""Test rapido de modelos disponibles con la cuenta paga."""
import os
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=api_key)

CANDIDATOS = [
    "gemini-2.0-flash-001",
    "gemini-2.0-flash-lite",
    "gemini-2.0-flash-lite-001",
    "gemini-2.5-flash-lite",
    "gemini-flash-latest",
    "gemini-flash-lite-latest",
    "gemini-pro-latest",
    "gemini-2.5-pro",
    "gemini-3-flash-preview",
]

print("Probando modelos con cuenta paga...\n")
for modelo in CANDIDATOS:
    try:
        r = client.models.generate_content(
            model=modelo,
            contents="Responde solo: OK",
            config=types.GenerateContentConfig(max_output_tokens=5)
        )
        print(f"  OK  {modelo}  -> {r.text.strip()}")
    except Exception as e:
        msg = str(e)[:120]
        print(f"  FAIL {modelo}  -> {msg}")
