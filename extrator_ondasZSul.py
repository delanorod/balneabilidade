import requests
import json
import time
from datetime import date


# =========================================
# FUNÇÕES AUXILIARES
# =========================================

def classificar_agitacao(altura_m: float) -> str:
    if altura_m < 0.5:   return "Fraco"
    elif altura_m < 1.25: return "Moderado"
    elif altura_m < 2.5:  return "Forte"
    else:                 return "Muito Forte"


def graus_para_direcao(graus: float) -> str:
    dirs = ["N", "NE", "L", "SE", "S", "SO", "O", "NO"]
    return dirs[round(graus / 45) % 8]


# =========================================
# LEITURA DO JSON
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
        praias.append({"nome": nome, "lat": p.get("lat"), "lon": p.get("lon")})
    return praias


# =========================================
# OPEN-METEO: ONDAS (Marine API)
# =========================================

def buscar_ondas(lat: float, lon: float, hoje: str) -> dict:
    """
    Marine API - única fonte de wave_height e direção de ondas.
    wind_speed_10m NÃO existe aqui; usar Weather API para vento.
    https://open-meteo.com/en/docs/marine-weather-api
    """
    url = "https://marine-api.open-meteo.com/v1/marine"
    params = {
        "latitude":      lat,
        "longitude":     lon,
        "daily":         ["wave_height_max", "wind_wave_direction_dominant"],
        "timezone":      "America/Sao_Paulo",
        "forecast_days": 1,
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()

    datas = data["daily"]["time"]
    idx = datas.index(hoje) if hoje in datas else 0
    onda = data["daily"]["wave_height_max"][idx]
    dir_graus = data["daily"]["wind_wave_direction_dominant"][idx]

    return {
        "onda":    round(onda, 2) if onda is not None else None,
        "direcao": graus_para_direcao(dir_graus) if dir_graus is not None else None,
    }


# =========================================
# OPEN-METEO: VENTO (Weather Forecast API)
# =========================================

def buscar_vento(lat: float, lon: float) -> float | None:
    """
    Weather Forecast API - wind_speed_10m só existe aqui, não na Marine API.
    https://open-meteo.com/en/docs
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":        lat,
        "longitude":       lon,
        "hourly":          "wind_speed_10m",
        "wind_speed_unit": "kmh",
        "timezone":        "America/Sao_Paulo",
        "forecast_days":   1,
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()

    ventos = [v for v in data["hourly"]["wind_speed_10m"] if v is not None]
    return round(sum(ventos) / len(ventos), 1) if ventos else None


# =========================================
# COMBINA ONDAS + VENTO
# =========================================

def buscar_previsao_ondas_openmeteo(lat: float, lon: float) -> dict | None:
    hoje = date.today().isoformat()
    try:
        ondas = buscar_ondas(lat, lon, hoje)
        onda  = ondas["onda"]
        vento = buscar_vento(lat, lon)
        return {
            "data":     hoje,
            "onda":     onda,
            "vento":    vento,
            "agitacao": classificar_agitacao(onda) if onda is not None else None,
            "direcao":  ondas["direcao"],
        }
    except Exception as e:
        print(f"  ⚠️  Erro ({lat},{lon}): {e}")
        return None


# =========================================
# FUNÇÃO PRINCIPAL
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

        time.sleep(0.2)

    return resultados


# =========================================
# EXECUCAO
# =========================================

if __name__ == "__main__":
    dados = extrair_dados()
    print("\nRESULTADO:")
    print(json.dumps(dados[:5], indent=2, ensure_ascii=False))
