import json, sys
sys.stdout.reconfigure(encoding='utf-8')

convs: dict[str, list] = {}
with open(r'D:\DatosDeMercado\marketing_data\instagram_20260802\processed\chat_corpus_nlp.jsonl', 'r', encoding='utf-8') as f:
    for line in f:
        d = json.loads(line.strip())
        convs.setdefault(d['conversation_id'], []).append(d)

# Debug arrebol
print("=== arrebolaikeaimar ===")
for m in convs.get('arrebolaikeaimar_1349004260747692', []):
    if m['sender_name'] != 'Tomas Santiago Cueva':
        print(f"  [{m['sender_name']}]: {repr(m['full_text'])}")

# Debug eztevae
print("\n=== eztevae ===")
for m in convs.get('eztevae_943487714817654', []):
    if m['sender_name'] != 'Tomas Santiago Cueva':
        print(f"  [{m['sender_name']}]: {repr(m['full_text'][:200])}")
