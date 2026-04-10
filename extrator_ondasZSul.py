import requests
import json
import time
from datetime import datetime, date


# =========================================
# 🔧 FUNÇÕES AUXILIARES
# =========================================

def classificar_agitacao(altura_m: float) -> str:
    """Converte altura de onda em classificação textual equivalente ao CPTEC."""
    if altura_m < 0.5:
        return "Fraco"
    elif altura_m < 1.25:
        return "Moderado"
    elif altura_m < 2.5:
        return "Forte"
    else:
        return "Muito Forte"


def graus_para_direcao(graus: float) -> str:
    """Converte graus em direção cardinal."""
    dirs = ["N", "NE", "L", "SE", "S", "SO", "O", "NO"]
    idx = round(graus / 45) % 8
    return dirs[idx]


# =========================================
# 📥 LEITURA DO JSON
# =========================================

def carregar_praias_json():
    with open("praias_rj.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("praias", [])


def montar_praias():
    praias_json = carregar_praias_json()
    praias = []
    nomes = set()

    for p in praias_json:
        nome = p["nome"].strip()
        if nome in nomes:
            continue
        nomes.add(nome)
        praias.append({
            "nome": nome,
            "lat":  p.get("lat"),
            "lon":  p.get("lon"),
        })

    return praias


# =========================================
# 🌊 OPEN-METEO MARINE API
# =========================================

def buscar_previsao_ondas_openmeteo(lat: float, lon: float) -> dict | None:
    """
    Consulta a Open-Meteo Marine API para as coordenadas dadas.
    Retorna dados do dia de hoje: altura de onda, velocidade do vento e direção.

    Documentação: https://open-meteo.com/en/docs/marine-weather-api
    """
    url = "https://marine-api.open-meteo.com/v1/marine"
    params = {
        "latitude":    lat,
        "longitude":   lon,
        "daily": [
            "wave_height_max",
            "wind_wave_direction_dominant",
        ],
        "hourly": [
            "wind_speed_10m",   # vento em km/h (camada superficial)
        ],
        "wind_speed_unit": "kmh",
        "timezone":    "America/Sao_Paulo",
        "forecast_days": 1,
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        hoje = date.today().isoformat()

        # --- Onda máxima do dia ---
        datas_daily = data["daily"]["time"]
        idx_hoje = datas_daily.index(hoje) if hoje in datas_daily else 0

        onda   = data["daily"]["wave_height_max"][idx_hoje]
        dir_graus = data["daily"]["wind_wave_direction_dominant"][idx_hoje]
        direcao = graus_para_direcao(dir_graus) if dir_graus is not None else None

        # --- Vento: média das horas do dia de hoje ---
        ventos_hora = data["hourly"]["wind_speed_10m"]
        # Open-Meteo retorna 24 valores por dia; fatia as 24 horas do dia pedido
        inicio = idx_hoje * 24
        ventos_hoje = [v for v in ventos_hora[inicio:inicio + 24] if v is not None]
        vento = round(sum(ventos_hoje) / len(ventos_hoje), 1) if ventos_hoje else None

        agitacao = classificar_agitacao(onda) if onda is not None else None

        return {
            "data":     hoje,
            "onda":     round(onda, 2) if onda is not None else None,
            "vento":    vento,
            "agitacao": agitacao,
            "direcao":  direcao,
        }

    except Exception as e:
        print(f"  ⚠️  Open-Meteo erro ({lat},{lon}): {e}")
        return None


# =========================================
# 🚀 FUNÇÃO PRINCIPAL
# =========================================

def extrair_dados():
    praias = montar_praias()
    resultados = []

    for praia in praias:
        lat = praia.get("lat")
        lon = praia.get("lon")

        if lat is None or lon is None:
            print(f"  ⚠️  {praia['nome']}: sem coordenadas, pulando.")
            continue

        print(f"  Coletando: {praia['nome']} ({lat}, {lon})")
        previsao = buscar_previsao_ondas_openmeteo(lat, lon)

        if not previsao:
            continue

        resultados.append({
            "nome":     praia["nome"],
            "lat":      lat,
            "lon":      lon,
            "onda":     previsao["onda"],
            "vento":    previsao["vento"],
            "agitacao": previsao["agitacao"],
            "direcao":  previsao["direcao"],
            "data":     previsao["data"],
        })

        time.sleep(0.2)  # Open-Meteo é generosa, mas respeite o rate limit

    return resultados


# =========================================
# ▶️ EXECUÇÃO
# =========================================

if __name__ == "__main__":
    dados = extrair_dados()
    print("\nRESULTADO:")
    print(json.dumps(dados[:5], indent=2, ensure_ascii=False))
