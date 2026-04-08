# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 16:10:32 2026

@author: Delano
"""

import json
from datetime import datetime

import extrator_ondasZSul
from praiascrapper2 import scrape_balneabilidade

import re

def normalizar_nome(nome):
    # Remove sufixos entre parênteses: "Copacabana (Posto 2)" → "Copacabana"
    nome = re.sub(r'\s*\(.*?\)', '', nome).strip()
    # Apelidos conhecidos
    apelidos = {
        "recreio dos bandeirantes": "Recreio",
        "barra": "Barra da Tijuca",
    }
    return apelidos.get(nome.lower(), nome)

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

ondas_lista = extrator_ondasZSul.main()

if not ondas_lista:
    print("⚠️ Nenhum dado de ondas retornado")
    ondas_lista = []

# FIX: ondas_dict deve ficar FORA do bloco if, senão nunca é criado com dados reais
ondas_dict = {
    o["nome"].strip().lower(): o
    for o in ondas_lista
}


# -----------------------------------------
# COLETAR BALNEABILIDADE
# -----------------------------------------
print("Coletando dados de balneabilidade...")

bal_lista = scrape_balneabilidade()
fonte_balneabilidade = "praialimpa.net"

print(f"✅ Balneabilidade coletada: {len(bal_lista)} praias")

balneabilidade = {}

for item in bal_lista:

    nome = item["praia"].strip()
    nome = nome.split(" - ")[0]

    balneabilidade[nome] = {
        "status": item["status"],
        "data_coleta": None,
        "observacoes": f"Região: {item.get('regiao')}"
    }
# -----------------------------------------
# MONTAR LISTA FINAL
# -----------------------------------------

dados_finais = []

for praia, bal_data in balneabilidade.items():

    status = (bal_data.get("status") or "").strip().lower()

    nome_normalizado = praia.strip().lower()
    dados_onda = ondas_dict.get(nome_normalizado)

    if dados_onda:
        onda = dados_onda.get("onda")
        vento = dados_onda.get("vento")
        lat = dados_onda.get("lat")
        lon = dados_onda.get("lon")
    else:
        onda = None
        vento = None
        # fallback: coordenadas do cadastro fixo
        coord = COORDENADAS.get(praia, {})
        lat = coord.get("lat")
        lon = coord.get("lon")

    score = calcular_score(onda, vento, status)

    dados_finais.append({
        "nome": praia,
        "lat": lat,
        "lon": lon,
        "onda": onda,
        "vento": vento,
        "balneabilidade": status,
        "data_coleta": bal_data.get("data_coleta"),
        "observacoes": bal_data.get("observacoes"),
        "score": score
    })

dados_finais.sort(key=lambda x: x["score"], reverse=True)


# -----------------------------------------
# CALCULAR PRAIA RECOMENDADA
# -----------------------------------------

if dados_finais:
    melhor_praia = max(dados_finais, key=lambda x: x["score"])
else:
    melhor_praia = {"nome": None}


# -----------------------------------------
# GERAR JSON FINAL
# -----------------------------------------

json_final = {
    "ultima_atualizacao": datetime.now().isoformat(),
    "fonte_balneabilidade": fonte_balneabilidade,
    "praia_recomendada": melhor_praia["nome"],
    "praias": dados_finais
}


# -----------------------------------------
# SALVAR JSON
# -----------------------------------------

with open("praias_rj.json", "w", encoding="utf-8") as f:
    json.dump(json_final, f, indent=2, ensure_ascii=False)

print("✅ JSON atualizado com sucesso!")
print(f"🏖️  Praia recomendada hoje: {melhor_praia['nome']}")
