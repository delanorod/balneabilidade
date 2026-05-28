# -*- coding: utf-8 -*-
"""
extrator_copernicus.py  —  v1.0
================================
Substitui extrator_ondasZSul.py usando Copernicus Marine Service.

Estratégia:
  • Ondas (altura + direção) : Copernicus Marine — ÚNICO download para toda a
    região do RJ, depois extrai o ponto mais próximo de cada praia via xarray.
  • Vento (10 m)             : Open-Meteo Forecast API (não disponível no
    dataset de ondas do Copernicus Marine).

Retorna:
  Lista de dicts compatível com o contrato esperado por gerar_json_praias.py:
    [{"nome", "lat", "lon", "onda", "vento", "agitacao", "direcao", "data"}, ...]

Dependências:
  pip install copernicusmarine xarray numpy requests
"""

import time
import requests
import numpy as np
from datetime import datetime, timedelta, date

import copernicusmarine
import xarray as xr

# Importa catálogo de praias do INEAScraper (fonte canônica de coordenadas)
from inea_scraper2 import INEAScraper


# =========================================================================
# CONFIGURAÇÕES
# =========================================================================

# Dataset de ondas do Copernicus Marine (resolução 0.083° ~9 km, passo 3 h)
WAVES_DATASET_ID = "cmems_mod_glo_wav_anfc_0.083deg_PT3H-i"

# Bounding box que cobre todas as praias do catálogo (RJ + Niterói)
# Margem extra de 0.2° para garantir que o grid contenha os pontos de borda
LONGITUDE_MIN = -43.70
LONGITUDE_MAX = -43.00
LATITUDE_MIN  = -23.20
LATITUDE_MAX  = -22.85


# =========================================================================
# HELPERS
# =========================================================================

def classificar_agitacao(altura_m: float) -> str:
    """Classifica o mar pelo critério de altura significativa."""
    if altura_m < 0.5:    return "Fraco"
    elif altura_m < 1.25: return "Moderado"
    elif altura_m < 2.5:  return "Forte"
    else:                  return "Muito Forte"


def graus_para_direcao(graus: float) -> str:
    """Converte graus meteorológicos em sigla cardinal (8 pontos)."""
    dirs = ["N", "NE", "L", "SE", "S", "SO", "O", "NO"]
    return dirs[round(graus / 45) % 8]


def _valor_seguro(arr) -> float | None:
    """Extrai escalar de array/DataArray; retorna None se NaN."""
    try:
        v = float(np.nanmean(arr.values))
        return None if np.isnan(v) else v
    except Exception:
        return None


# =========================================================================
# COPERNICUS — download único para toda a região
# =========================================================================

def baixar_ondas_copernicus(start: str, end: str) -> xr.Dataset | None:
    """
    Baixa VHM0 (altura significativa) e VMDR (direção média) para o
    bounding box do RJ. Um único download cobre todas as praias.

    Args:
        start: data inicial no formato 'YYYY-MM-DD'
        end:   data final  no formato 'YYYY-MM-DD'

    Returns:
        xr.Dataset com dimensões (time, latitude, longitude) ou None em erro.
    """
    print(f"🌊 [Copernicus] Baixando ondas {start} → {end} ...")
    try:
        ds = copernicusmarine.subset(
            dataset_id=WAVES_DATASET_ID,
            variables=["VHM0", "VMDR"],          # altura significativa + direção
            minimum_longitude=LONGITUDE_MIN,
            maximum_longitude=LONGITUDE_MAX,
            minimum_latitude=LATITUDE_MIN,
            maximum_latitude=LATITUDE_MAX,
            start_datetime=start,
            end_datetime=end,
        )
        print(f"  ✅ Download concluído. Grid: {dict(ds.dims)}")
        return ds
    except Exception as exc:
        print(f"  ❌ Erro ao baixar Copernicus: {exc}")
        return None


def extrair_ponto(ds: xr.Dataset, lat: float, lon: float, varname: str) -> float | None:
    """
    Seleciona o ponto de grade mais próximo de (lat, lon) e retorna a
    média temporal do dia como escalar Python.
    """
    try:
        ponto = ds[varname].sel(latitude=lat, longitude=lon, method="nearest")
        return _valor_seguro(ponto)
    except Exception as exc:
        print(f"  ⚠️  extrair_ponto({varname}, {lat}, {lon}): {exc}")
        return None


# =========================================================================
# OPEN-METEO — vento de superfície (10 m)
# =========================================================================

def buscar_vento_openmeteo(lat: float, lon: float, timeout: int = 20) -> float | None:
    """
    Média diária do vento a 10 m (km/h) via Open-Meteo Forecast API.
    Separado do Copernicus porque o dataset de ondas não inclui vento atmosférico.
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
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        ventos = [v for v in r.json()["hourly"]["wind_speed_10m"] if v is not None]
        return round(sum(ventos) / len(ventos), 1) if ventos else None
    except Exception as exc:
        print(f"  ⚠️  Vento Open-Meteo ({lat},{lon}): {exc}")
        return None


# =========================================================================
# FUNÇÃO PRINCIPAL — interface pública
# =========================================================================

def extrair_dados() -> list[dict]:
    """
    Coleta ondas (Copernicus) + vento (Open-Meteo) para todas as praias
    do catálogo do INEAScraper.

    Retorna lista de dicts:
      {"nome", "lat", "lon", "onda", "vento", "agitacao", "direcao", "data"}

    Compatível com o contrato de extrator_ondasZSul.extrair_dados().
    """
    hoje = date.today()
    hoje_str  = hoje.isoformat()
    amanha_str = (hoje + timedelta(days=1)).isoformat()

    # ── 1. Catálogo de praias (coordenadas canônicas) ─────────────────
    scraper_inea = INEAScraper()
    praias_catalog = scraper_inea.PRAIAS          # dict {id: {nome, coordenadas, ...}}

    # ── 2. Download único de ondas para toda a região ─────────────────
    ds = baixar_ondas_copernicus(hoje_str, amanha_str)

    resultados = []

    for pid, info in praias_catalog.items():
        coord = info.get("coordenadas")
        if not coord:
            print(f"  ⚠️  {info['nome']}: sem coordenadas, pulando.")
            continue

        lat  = coord["latitude"]
        lon  = coord["longitude"]
        nome = info["nome"]

        print(f"  📍 {nome} ({lat}, {lon})")

        # ── 3a. Ondas do Copernicus (ou None se download falhou) ──────
        if ds is not None:
            onda      = extrair_ponto(ds, lat, lon, "VHM0")
            dir_graus = extrair_ponto(ds, lat, lon, "VMDR")
            onda      = round(onda, 2) if onda is not None else None
            direcao   = graus_para_direcao(dir_graus) if dir_graus is not None else None
        else:
            onda    = None
            direcao = None

        # ── 3b. Vento do Open-Meteo ───────────────────────────────────
        vento = buscar_vento_openmeteo(lat, lon)
        time.sleep(0.15)           # respeita rate-limit da Open-Meteo

        resultados.append({
            "nome":     nome,
            "lat":      lat,
            "lon":      lon,
            "onda":     onda,
            "vento":    vento,
            "agitacao": classificar_agitacao(onda) if onda is not None else None,
            "direcao":  direcao,
            "data":     hoje_str,
        })

    print(f"\n✅ extrator_copernicus: {len(resultados)} praias processadas.")
    return resultados


# =========================================================================
# EXECUÇÃO DIRETA (teste isolado)
# =========================================================================

if __name__ == "__main__":
    import json
    dados = extrair_dados()
    print("\n--- Amostra (5 primeiras) ---")
    print(json.dumps(dados[:5], indent=2, ensure_ascii=False))
