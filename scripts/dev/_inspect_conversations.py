import json
import sys

sys.stdout.reconfigure(encoding='utf-8')

convs: dict[str, list] = {}
with open(r'D:\DatosDeMercado\marketing_data\instagram_20260802\processed\chat_corpus_nlp.jsonl', 'r', encoding='utf-8') as f:
    for line in f:
        d = json.loads(line.strip())
        cid = d['conversation_id']
        if cid not in convs:
            convs[cid] = []
        convs[cid].append(d)

# Mostrar una conversacion con >5 mensajes como ejemplo
for cid, msgs in convs.items():
    if len(msgs) > 5:
        print(f'=== {cid} ({len(msgs)} msgs) ===')
        for m in msgs[:8]:
            print(f"  [{m['sender_name']}]: {m['full_text'][:120]}")
        print()
        break

# Stats
print(f'Total conversations: {len(convs)}')
lens = [len(v) for v in convs.values()]
print(f'Msgs per conv - min: {min(lens)}, max: {max(lens)}, avg: {sum(lens)/len(lens):.1f}')
print(f'Keys in record: {list(convs[list(convs.keys())[0]][0].keys())}')
