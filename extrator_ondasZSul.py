import requests
import json
import time
from datetime import datetime

# URL do JSON de balneabilidade (GitHub raw)
URL_BALNEABILIDADE = "https://raw.githubusercontent.com/delanorod/balneabilidade/main/balneabilidade.json"

def carregar_praias():
    response = requests.get(URL_BALNEABILIDADE)
    data = response.json()

    praias = []

    for item in data:
        praias.append({
            "nome": item.get("praia"),
            "lat": item.get("lat"),
            "lon": item.get("lon"),
            "status": item.get("qualidade")  # própria/imprópria
        })

    return praias


def obter_dados_ondas(lat, lon):
    url = f"https://marine-api.open-meteo.com/v1/marine?latitude={lat}&longitude={lon}&current=wave_height,wind_speed"

    response = requests.get(url)
    data = response.json()

    current = data.get("current", {})

    return {
        "onda": current.get("wave_height", 0),
        "vento": current.get("wind_speed", 0)
    }


def calcular_score(onda, vento):
    score = 0

    # exemplo simples (você pode evoluir depois)
    if 0.5 <= onda <= 1.5:
        score += 5
    if vento < 15:
        score += 5

    return score


def main():
    praias = carregar_praias()
    resultado = []

    for praia in praias:
        if not praia["lat"] or not praia["lon"]:
            continue

        dados = obter_dados_ondas(praia["lat"], praia["lon"])

        time.sleep(1)

        score = calcular_score(dados["onda"], dados["vento"])

        resultado.append({
            "nome": praia["nome"],
            "lat": praia["lat"],
            "lon": praia["lon"],
            "onda": dados["onda"],
            "vento": dados["vento"],
            "score": score,
            "balneabilidade": praia["status"],
            "ultima_atualizacao": datetime.utcnow().isoformat()
        })

    # salva no arquivo usado pelo app
    with open("praias_rj.json", "w", encoding="utf-8") as f:
        json.dump({
            "ultima_atualizacao": datetime.utcnow().isoformat(),
            "praias": resultado
        }, f, ensure_ascii=False, indent=2)

    print("✅ Arquivo atualizado com sucesso!")


if __name__ == "__main__":
    main()