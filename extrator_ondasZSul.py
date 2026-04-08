import requests
import json
import time
from datetime import datetime

URL_BALNEABILIDADE = "https://raw.githubusercontent.com/delanorod/balneabilidade/main/balneabilidade.json"

def carregar_praias():
    print("\n[DEBUG] Buscando JSON de balneabilidade...")
    print(f"[DEBUG] URL: {URL_BALNEABILIDADE}")

    try:
        response = requests.get(URL_BALNEABILIDADE, timeout=10)
        print(f"[DEBUG] Status HTTP: {response.status_code}")

        if response.status_code != 200:
            print(f"[DEBUG] ❌ Resposta inesperada: {response.text[:200]}")
            return []

        data = response.json()
        print(f"[DEBUG] Total de itens no JSON: {len(data)}")
        print(f"[DEBUG] Amostra (3 primeiros):")
        for item in data[:3]:
            print(f"         {item}")

    except Exception as e:
        print(f"[DEBUG] ❌ Erro ao carregar balneabilidade: {e}")
        return []

    praias = []
    sem_coordenada = 0

    for item in data:
        lat = item.get("lat")
        lon = item.get("lon")

        if not lat or not lon:
            sem_coordenada += 1
            continue

        praias.append({
            "nome": item.get("praia"),
            "lat": lat,
            "lon": lon,
            "status": item.get("qualidade")
        })

    print(f"[DEBUG] Praias com coordenadas: {len(praias)} | Sem coordenada: {sem_coordenada}")
    return praias


def obter_dados_ondas(lat, lon):
    url = (
        f"https://marine-api.open-meteo.com/v1/marine"
        f"?latitude={lat}&longitude={lon}&current=wave_height,wind_speed"
    )
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        current = data.get("current", {})
        return {
            "onda": current.get("wave_height", 0),
            "vento": current.get("wind_speed", 0)
        }
    except Exception as e:
        print(f"[DEBUG] ❌ Erro ao buscar ondas ({lat},{lon}): {e}")
        return {"onda": None, "vento": None}


def calcular_score(onda, vento):
    score = 0
    if onda is not None and 0.5 <= onda <= 1.5:
        score += 5
    if vento is not None and vento < 15:
        score += 5
    return score


def main():
    praias = carregar_praias()

    if not praias:
        print("[DEBUG] ❌ Nenhuma praia carregada. Abortando.")
        return []

    resultado = []
    erros = 0

    print(f"\n[DEBUG] Iniciando coleta de ondas para {len(praias)} praias...")

    for i, praia in enumerate(praias):
        lat = praia["lat"]
        lon = praia["lon"]

        dados = obter_dados_ondas(lat, lon)

        if dados["onda"] is None:
            erros += 1

        onda = dados.get("onda", 0)
        vento = dados.get("vento", 0)
        score = calcular_score(onda, vento)

        resultado.append({
            "nome": praia["nome"],
            "lat": lat,
            "lon": lon,
            "onda": onda,
            "vento": vento,
            "score": score,
        })

        # Amostra nas 3 primeiras, a cada 5, e na última
        if i < 3 or (i + 1) % 5 == 0 or i == len(praias) - 1:
            print(f"[DEBUG] [{i+1}/{len(praias)}] {praia['nome']} | "
                  f"onda={onda}m | vento={vento}km/h | score={score}")

        time.sleep(1)

    print(f"\n[DEBUG] ✅ Coleta concluída: {len(resultado)} praias | {erros} erros de ondas")
    print(f"[DEBUG] Amostra do resultado final (3 primeiras):")
    for p in resultado[:3]:
        print(f"         {p}")

    with open("praias_rj.json", "w", encoding="utf-8") as f:
        json.dump({
            "ultima_atualizacao": datetime.utcnow().isoformat(),
            "praias": resultado
        }, f, ensure_ascii=False, indent=2)

    print("[DEBUG] Arquivo praias_rj.json salvo.")
    return resultado


if __name__ == "__main__":
    main()
