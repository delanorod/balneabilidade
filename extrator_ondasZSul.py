import requests
import json
import time
from datetime import date


# =========================================
# FUNÇÕES AUXILIARES
# =========================================

def classificar_agitacao(altura_m: float) -> str:
    if altura_m < 0.5:    return "Fraco"
    elif altura_m < 1.25: return "Moderado"
    elif altura_m < 2.5:  return "Forte"
    else:                 return "Muito Forte"


def graus_para_direcao(graus: float) -> str:
    dirs = ["N", "NE", "L", "SE", "S", "SO", "O", "NO"]
    return dirs[round(graus / 45) % 8]


# =========================================
# LEITURA DO JSON  (usado apenas quando
# extrator é executado de forma autônoma,
# sem receber praias via parâmetro)
# =========================================

def carregar_praias_json(caminho: str = "praias_rj.json") -> list:
    """Lê lista de praias do JSON gerado anteriormente."""
    try:
        with open(caminho, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("praias", [])
    except FileNotFoundError:
        print(f"  ⚠️  '{caminho}' não encontrado. "
              "Passe a lista de praias diretamente via extrair_dados(praias=[...]).")
        return []


def _praias_do_json(caminho: str = "praias_rj.json") -> list[dict]:
    """Monta lista [{nome, lat, lon}] a partir do JSON salvo."""
    praias_json = carregar_praias_json(caminho)
    praias, nomes = [], set()
    for p in praias_json:
        nome = p["nome"].strip()
        if nome in nomes:
            continue
        nomes.add(nome)
        praias.append({"nome": nome, "lat": p.get("lat"), "lon": p.get("lon")})
    return praias


# =========================================
# RETRY COM BACKOFF EXPONENCIAL
# =========================================

def get_com_retry(
    url: str,
    params: dict,
    timeout: int = 30,
    max_tentativas: int = 3,
) -> requests.Response:
    for tentativa in range(1, max_tentativas + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if tentativa == max_tentativas:
                raise
            espera = 2 ** tentativa
            print(f"    ↻ Tentativa {tentativa}/{max_tentativas} falhou "
                  f"({e.__class__.__name__}), aguardando {espera}s...")
            time.sleep(espera)


# =========================================
# OPEN-METEO: ONDAS (Marine API)
# =========================================

def buscar_ondas(lat: float, lon: float, hoje: str) -> dict:
    url = "https://marine-api.open-meteo.com/v1/marine"
    params = {
        "latitude":      lat,
        "longitude":     lon,
        "daily":         ["wave_height_max", "wind_wave_direction_dominant"],
        "timezone":      "America/Sao_Paulo",
        "forecast_days": 1,
    }
    r = get_com_retry(url, params)
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
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":        lat,
        "longitude":       lon,
        "hourly":          "wind_speed_10m",
        "wind_speed_unit": "kmh",
        "timezone":        "America/Sao_Paulo",
        "forecast_days":   1,
    }
    r = get_com_retry(url, params)
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

def extrair_dados(praias: list[dict] | None = None) -> list[dict]:
    """
    Coleta ondas e vento para cada praia via Open-Meteo.

    Parâmetros
    ----------
    praias : list[dict] | None
        Lista de dicts com chaves 'nome', 'lat', 'lon'.
        Se None, tenta ler do praias_rj.json (modo autônomo).
        Quando chamado por gerar_json_praias.py, passe a lista
        montada a partir do INEAScraper.PRAIAS para evitar
        dependência circular com o JSON ainda não gerado.

    Retorna
    -------
    list[dict]  — cada item: {nome, lat, lon, onda, vento,
                               agitacao, direcao, data}
    """
    if praias is None:
        praias = _praias_do_json()

    if not praias:
        print("  ⚠️  Lista de praias vazia. Nenhum dado coletado.")
        return []

    resultados = []

    for praia in praias:
        nome = praia.get("nome", "?")
        lat  = praia.get("lat")
        lon  = praia.get("lon")

        if lat is None or lon is None:
            print(f"  ⚠️  {nome}: sem coordenadas, pulando.")
            continue

        print(f"  Coletando: {nome} ({lat}, {lon})")
        previsao = buscar_previsao_ondas_openmeteo(lat, lon)

        if not previsao:
            continue

        resultados.append({
            "nome":     nome,
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
# EXECUÇÃO AUTÔNOMA
# =========================================

if __name__ == "__main__":
    dados = extrair_dados()   # lê praias_rj.json quando rodado sozinho
    print("\nRESULTADO:")
    print(json.dumps(dados[:5], indent=2, ensure_ascii=False))
