# -*- coding: utf-8 -*-
"""
Created on Thu Feb 19 13:54:58 2026

@author: Delano
"""

# -*- coding: utf-8 -*-
"""
Previsão de Ondas e Vento - Praias da Zona Sul (RJ)
Atualizado: 2026
@author: Delano (melhorado com Claude)
"""

import requests
import pandas as pd
from datetime import datetime

# ─────────────────────────────────────────────
# Praias da Zona Sul do Rio de Janeiro
# ─────────────────────────────────────────────
PRAIAS = {
    "Copacabana":  {"lat": -22.9667, "lon": -43.1827},
    "Arpoador":    {"lat": -22.9900, "lon": -43.1900},
    "Ipanema":     {"lat": -22.9876, "lon": -43.2009},
    "Leblon":      {"lat": -22.9864, "lon": -43.2228},
    "São Conrado": {"lat": -23.0217, "lon": -43.2844},
    "Barra":       {"lat": -23.0122, "lon": -43.3650},
}

# ─────────────────────────────────────────────
# Utilitários
# ─────────────────────────────────────────────
SETORES = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
           "S", "SSO", "SO", "OSO", "O", "ONO", "NO", "NNO"]

def graus_para_cardeal(graus):
    if pd.isna(graus):
        return "—"
    idx = int((float(graus) + 11.25) / 22.5) % 16
    return SETORES[idx]

def classificar_onda(altura):
    if pd.isna(altura):
        return "—"
    h = float(altura)
    if h < 0.5:   return "Calmo"
    if h < 1.0:   return "Fraco"
    if h < 1.5:   return "Moderado"
    if h < 2.5:   return "Bom"
    if h < 3.5:   return "Grande"
    return "Muito Grande"

def classificar_vento(velocidade_kmh):
    if pd.isna(velocidade_kmh):
        return "—"
    v = float(velocidade_kmh)
    if v < 10:  return "Calmo"
    if v < 20:  return "Leve"
    if v < 30:  return "Moderado"
    if v < 50:  return "Forte"
    return "Muito Forte"

# ─────────────────────────────────────────────
# Busca de dados via Open-Meteo
# ─────────────────────────────────────────────
def buscar_dados(lat, lon, forecast_days=3):
    # --- Ondas (Marine API) ---
    url_marine = "https://marine-api.open-meteo.com/v1/marine"
    params_marine = {
        "latitude": lat,
        "longitude": lon,
        "hourly": [
            "wave_height",
            "wave_period",
            "wave_direction",
            "swell_wave_height",
            "swell_wave_period",
            "swell_wave_direction",
            "wind_wave_height",
        ],
        "timezone": "America/Sao_Paulo",
        "forecast_days": forecast_days,
    }

    # --- Vento (Forecast API) ---
    url_forecast = "https://api.open-meteo.com/v1/forecast"
    params_forecast = {
        "latitude": lat,
        "longitude": lon,
        "hourly": [
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
        ],
        "wind_speed_unit": "kmh",
        "timezone": "America/Sao_Paulo",
        "forecast_days": forecast_days,
    }

    resp_marine   = requests.get(url_marine,   params=params_marine,   timeout=15)
    resp_forecast = requests.get(url_forecast, params=params_forecast, timeout=15)

    if resp_marine.status_code != 200:
        raise RuntimeError(f"Erro Marine API: {resp_marine.status_code} – {resp_marine.text}")
    if resp_forecast.status_code != 200:
        raise RuntimeError(f"Erro Forecast API: {resp_forecast.status_code} – {resp_forecast.text}")

    df_marine   = pd.DataFrame(resp_marine.json()["hourly"])
    df_forecast = pd.DataFrame(resp_forecast.json()["hourly"])

    df = pd.merge(df_marine, df_forecast, on="time")
    df["time"] = pd.to_datetime(df["time"])
    return df

# ─────────────────────────────────────────────
# Enriquecimento dos dados
# ─────────────────────────────────────────────
def enriquecer(df):
    # Direções em cardeal
    df["onda_dir_cardeal"]    = df["wave_direction"].apply(graus_para_cardeal)
    df["swell_dir_cardeal"]   = df["swell_wave_direction"].apply(graus_para_cardeal)
    df["vento_dir_cardeal"]   = df["wind_direction_10m"].apply(graus_para_cardeal)

    # Classificações qualitativas
    df["cond_onda"]  = df["wave_height"].apply(classificar_onda)
    df["cond_vento"] = df["wind_speed_10m"].apply(classificar_vento)

    # Renomear colunas para exibição amigável
    df = df.rename(columns={
        "time":                "Hora",
        "wave_height":         "Onda (m)",
        "wave_period":         "Período (s)",
        "wave_direction":      "Onda Dir (°)",
        "onda_dir_cardeal":    "Onda Dir",
        "swell_wave_height":   "Swell (m)",
        "swell_wave_period":   "Swell Per (s)",
        "swell_wave_direction":"Swell Dir (°)",
        "swell_dir_cardeal":   "Swell Dir",
        "wind_wave_height":    "Mar Vento (m)",
        "wind_speed_10m":      "Vento (km/h)",
        "wind_direction_10m":  "Vento Dir (°)",
        "vento_dir_cardeal":   "Vento Dir",
        "wind_gusts_10m":      "Rajada (km/h)",
        "cond_onda":           "Condição Onda",
        "cond_vento":          "Condição Vento",
    })
    return df

# ─────────────────────────────────────────────
# Relatório por praia
# ─────────────────────────────────────────────
COLUNAS_EXIBIR = [
    "Hora", "Onda (m)", "Período (s)", "Onda Dir", "Condição Onda",
    "Swell (m)", "Swell Per (s)", "Swell Dir",
    "Vento (km/h)", "Rajada (km/h)", "Vento Dir", "Condição Vento",
]

def imprimir_relatorio(praia, df, horas=12):
    separador = "═" * 110
    print(f"\n{separador}")
    print(f"  🏄 {praia.upper()} — Próximas {horas}h  |  Gerado: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(separador)

    sub = df[COLUNAS_EXIBIR].head(horas).copy()
    sub["Hora"] = sub["Hora"].dt.strftime("%d/%m %H:%M")

    # Formata floats
    for col in ["Onda (m)", "Swell (m)", "Mar Vento (m)", "Vento (km/h)", "Rajada (km/h)", "Período (s)", "Swell Per (s)"]:
        if col in sub.columns:
            sub[col] = sub[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "—")

    print(sub.to_string(index=False))
    print()

# ─────────────────────────────────────────────
# Exportação para CSV (opcional)
# ─────────────────────────────────────────────
def exportar_csv(resultados: dict, caminho="previsao_zona_sul.csv"):
    frames = []
    for praia, df in resultados.items():
        df_copia = df.copy()
        df_copia.insert(0, "Praia", praia)
        frames.append(df_copia)
    pd.concat(frames, ignore_index=True).to_csv(caminho, index=False, encoding="utf-8-sig")
    print(f"✔ CSV exportado: {caminho}")

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main(horas_exibicao=12, forecast_days=3, exportar=False):
    resultados = {}
    for praia, coords in PRAIAS.items():
        try:
            print(f"Buscando dados: {praia}...", end=" ", flush=True)
            df_raw = buscar_dados(coords["lat"], coords["lon"], forecast_days)
            df     = enriquecer(df_raw)
            resultados[praia] = df
            print("OK")
        except Exception as e:
            print(f"ERRO — {e}")

    for praia, df in resultados.items():
        imprimir_relatorio(praia, df, horas=horas_exibicao)

    if exportar:
        exportar_csv(resultados)

    return resultados


if __name__ == "__main__":
    dados = main(
        horas_exibicao=12,   # janela exibida no console
        forecast_days=3,     # dias de previsão buscados
        exportar=False,      # True para gerar CSV
    )