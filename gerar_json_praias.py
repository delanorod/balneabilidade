# -*- coding: utf-8 -*-
import json
import re
from datetime import datetime

import extrator_ondasZSul
from praiascrapper2 import scrape_balneabilidade

# -----------------------------------------
# NORMALIZAÇÃO DE NOMES
# -----------------------------------------

APELIDOS = {
    "recreio dos bandeirantes": "Recreio",
    "barra": "Barra da Tijuca",
}

def normalizar_nome(nome):
    # Remove sufixos entre parênteses: "Copacabana (Posto 2)" → "Copacabana"
    nome = re.sub(r'\s*\(.*?\)', '', nome).strip()
    return APELIDOS.get(nome.lower(), nome)

COORDENADAS = {
    "Copacabana": {"lat": -22.9711, "lon": -43.1822},
    "Arpoador":   {"lat": -22.9870, "lon": -43.1910},
    "Ipanema":    {"lat": -22.9836, "lon": -43.2045},
    "Leblon":     {"lat": -22.9896, "lon": -43.2249},
    "Barra da Tijuca": {"lat": -23.0016, "lon": -43.3659},
    "Recreio":    {"lat": -23.0293, "lon": -43.4800},
}

# -----------------------------------------
# CALCULAR SCORE DA PRAIA
# -----------------------------------------

def calcular_score(onda, vento, bal):
    score = 0
    if bal == "propria":
        score += 100
    elif bal == "impropria":
        score -= 100
    if onda is not None:
        if onda < 0.5:    score += 40
        elif onda < 1.0:  score += 30
        elif onda < 1.5:  score += 10
        else:             score -= 10
    if vento is not None:
        if vento < 10:    score += 30
        elif vento < 20:  score += 15
        elif vento > 30:  score -= 20
    return score

# -----------------------------------------
# COLETAR ONDAS
# -----------------------------------------

print("Coletando dados de ondas...")
ondas_lista = extrator_ondasZSul.main()

if not ondas_lista:
    print("⚠️ Nenhum dado de ondas retornado")
    ondas_lista = []

ondas_dict = {
    o["nome"].strip().lower(): o
    for o in ondas_lista
}
print(f"[DEBUG] ondas_dict tem {len(ondas_dict)} entradas")
print(f"[DEBUG] Chaves de ondas_dict: {list(ondas_dict.keys())[:5]}")

# -----------------------------------------
# COLETAR BALNEABILIDADE
# -----------------------------------------

print("\nColetando dados de balneabilidade...")
bal_lista = scrape_balneabilidade()
fonte_balneabilidade = "praialimpa.net"
print(f"✅ Balneabilidade coletada: {len(bal_lista)} praias")
print(f"[DEBUG] Amostra bal_lista (3 primeiros): {bal_lista[:3]}")

balneabilidade = {}
for item in bal_lista:
    nome_raw = item["praia"].strip()
    nome_raw = nome_raw.split(" - ")[0]           # remove sufixo " - alguma coisa"
    nome_norm = normalizar_nome(nome_raw)         # remove (Posto X), resolve apelidos
    balneabilidade[nome_norm] = {
        "status": item["status"],
        "data_coleta": None,
        "observacoes": f"Região: {item.get('regiao')}"
    }

print(f"[DEBUG] balneabilidade tem {len(balneabilidade)} entradas após normalização")
print(f"[DEBUG] Chaves de balneabilidade: {list(balneabilidade.keys())[:8]}")

# -----------------------------------------
# MONTAR LISTA FINAL
# -----------------------------------------

dados_finais = []
matches = 0
fallbacks = 0
sem_coord = 0

for praia, bal_data in balneabilidade.items():
    status = (bal_data.get("status") or "").strip().lower()

    # BUG CORRIGIDO: normalizar_nome() agora é chamado aqui também
    nome_normalizado = normalizar_nome(praia).strip().lower()
    dados_onda = ondas_dict.get(nome_normalizado)

    if dados_onda:
        matches += 1
        onda  = dados_onda.get("onda")
        vento = dados_onda.get("vento")
        lat   = dados_onda.get("lat")
        lon   = dados_onda.get("lon")
        print(f"[DEBUG] ✅ Match de ondas: '{praia}' → '{nome_normalizado}'")
    else:
        fallbacks += 1
        onda  = None
        vento = None
        coord = COORDENADAS.get(praia, {})
        lat   = coord.get("lat")
        lon   = coord.get("lon")
        if not lat:
            sem_coord += 1
            print(f"[DEBUG] ⚠️ Sem ondas e sem coord: '{praia}' (chave buscada: '{nome_normalizado}')")

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

print(f"\n[DEBUG] Resumo do merge:")
print(f"         ✅ Com dados de ondas: {matches}")
print(f"         🔁 Usou fallback de coord: {fallbacks - sem_coord}")
print(f"         ❌ Sem coord alguma: {sem_coord}")

dados_finais.sort(key=lambda x: x["score"], reverse=True)

# -----------------------------------------
# CALCULAR PRAIA RECOMENDADA
# -----------------------------------------

if dados_finais:
    melhor_praia = max(dados_finais, key=lambda x: x["score"])
else:
    melhor_praia = {"nome": None}

# -----------------------------------------
# GERAR E SALVAR JSON FINAL
# -----------------------------------------

json_final = {
    "ultima_atualizacao": datetime.now().isoformat(),
    "fonte_balneabilidade": fonte_balneabilidade,
    "praia_recomendada": melhor_praia["nome"],
    "praias": dados_finais
}

with open("praias_rj.json", "w", encoding="utf-8") as f:
    json.dump(json_final, f, indent=2, ensure_ascii=False)

print("\n✅ JSON atualizado com sucesso!")
print(f"🏖️  Praia recomendada hoje: {melhor_praia['nome']}")
