import json

dados = {
    "items": ["Lorem ipsum dolor sit amet, consectetur adipiscing elit."] * 20000
}

with open("arquivo_1mb.json", "w", encoding="utf-8") as f:
    json.dump(dados, f, ensure_ascii=False)

print("Arquivo criado: arquivo_1mb.json")

