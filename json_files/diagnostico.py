import json
import os
from collections import Counter

def encontrar_links_repetidos(json_data):
    links = [item["link"] for item in json_data]
    contagem_links = Counter(links)
    
    links_repetidos = {link: count for link, count in contagem_links.items() if count > 1}
    return links_repetidos

def contar_estatisticas(json_data):
    total = len(json_data)
    links_nao_nulos = sum(1 for item in json_data if item.get("link"))
    solicitados = sum(1 for item in json_data if item.get("solicitado"))
    baixados = sum(1 for item in json_data if item.get("baixado"))
    
    return {
        "Total de registros": total,
        "Links diferentes de null": links_nao_nulos,
        "Total solicitados": solicitados,
        "Total baixados": baixados
    }

# Exemplo de uso
if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "solicitacoes.json")
    
    with open(file_path, "r", encoding="utf-8") as file:
        json_exemplo = json.load(file)
    
    repetidos = encontrar_links_repetidos(json_exemplo)
    estatisticas = contar_estatisticas(json_exemplo)
    
    print("Links repetidos e suas contagens:")
    print(json.dumps(repetidos, indent=4, ensure_ascii=False))
    
    print("\nEstatísticas gerais:")
    print(json.dumps(estatisticas, indent=4, ensure_ascii=False))
