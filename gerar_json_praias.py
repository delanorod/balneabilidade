# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 16:10:32 2026

@author: Delano
"""

import json
from datetime import datetime

import extrator_ondasZSul
from extrator_ondasZSul import PRAIAS
from inea_scraper2 import INEAScraper

COORDENADAS = {
    "Copacabana": {"lat": -22.9711, "lon": -43.1822},
    "Arpoador": {"lat": -22.9870, "lon": -43.1910},
    "Ipanema": {"lat": -22.9836, "lon": -43.2045},
    "Leblon": {"lat": -22.9896, "lon": -43.2249},
    "Barra da Tijuca": {"lat": -23.0016, "lon": -43.3659},
    "Recreio": {"lat": -23.0293, "lon": -43.4800},
}

# -----------------------------------------
# CALCULAR SCORE DA PRAIA
# -----------------------------------------

def calcular_score(onda, vento, bal):

    score = 0

    # balneabilidade
    if bal == "propria":
        score += 100
    elif bal == "impropria":
        score -= 100

    # onda
    if onda is not None:
        if onda < 0.5:
            score += 40
        elif onda < 1.0:
            score += 30
        elif onda < 1.5:
            score += 10
        else:
            score -= 10

    # vento
    if vento is not None:
        if vento < 10:
            score += 30
        elif vento < 20:
            score += 15
        elif vento > 30:
            score -= 20

    return score


# -----------------------------------------
# COLETAR ONDAS
# -----------------------------------------

print("Coletando dados de ondas...")

ondas = extrator_ondasZSul.main()


# -----------------------------------------
# COLETAR BALNEABILIDADE
# -----------------------------------------

print("Coletando dados de balneabilidade...")

scraper = INEAScraper()
bal_lista = scraper.scrape_balneabilidade()

balneabilidade = {}

for item in bal_lista:

    balneabilidade[item.praia_nome] = {
        "status": item.status
    }
# -----------------------------------------
# COORDENADAS DAS PRAIAS
# -----------------------------------------

COORDENADAS = {
    "Copacabana": {"lat": -22.9711, "lon": -43.1822},
    "Ipanema": {"lat": -22.9836, "lon": -43.2045},
    "Leblon": {"lat": -22.9896, "lon": -43.2249},
    "Barra da Tijuca": {"lat": -23.0016, "lon": -43.3659},
    "Recreio": {"lat": -23.0293, "lon": -43.4800},
}


# -----------------------------------------
# MONTAR LISTA FINAL
# -----------------------------------------

dados_finais = []

for praia in PRAIAS.keys():
    coord = COORDENADAS.get(praia, {})

    lat = coord.get("lat")
    lon = coord.get("lon")

    df = ondas.get(praia)

    if df is not None and not df.empty:

        linha = df.iloc[0]

        onda = float(linha["Onda (m)"])
        vento = float(linha["Vento (km/h)"])

    else:

        onda = None
        vento = None

    bal = balneabilidade.get(praia, {})

    status = bal.get("status")

    score = calcular_score(onda, vento, status)

    dados_finais.append({
        "nome": praia,
        "onda": onda,
        "vento": vento,
        "balneabilidade": status,
        "score": score,
        "lat": lat,
        "lon": lon
    })


# -----------------------------------------
# CALCULAR PRAIA RECOMENDADA
# -----------------------------------------

melhor_praia = max(dados_finais, key=lambda x: x["score"])


# -----------------------------------------
# GERAR JSON FINAL
# -----------------------------------------

json_final = {
    "ultima_atualizacao": datetime.now().isoformat(),
    "praia_recomendada": melhor_praia["nome"],
    "praias": dados_finais
}


# -----------------------------------------
# SALVAR JSON
# -----------------------------------------

with open("praias_rj.json", "w", encoding="utf-8") as f:

    json.dump(json_final, f, indent=2, ensure_ascii=False)

print("JSON atualizado com sucesso!")
print("Praia recomendada hoje:", melhor_praia["nome"])