# -*- coding: utf-8 -*-
"""
Gera praias_rj.json combinando:
  - Ondas/vento : extrator_ondasZSul (CPTEC/INPE)
  - Balneabilidade: praiascrapper2 (praialimpa.net)

Estratégia de merge:
  A fonte autoritativa de nomes e coordenadas é o extrator_ondasZSul
  (lista PRAIAS). O scraper retorna nomes com variações (sufixos, postos),
  então para cada praia do extrator buscamos a melhor correspondência
  na balneabilidade usando correspondência por prefixo normalizado.
"""

import json
import re
from datetime import datetime

import extrator_ondasZSul
from praiascrapper2 import scrape_balneabilidade


# ---------------------------------------------------------------------------
# NORMALIZAÇÃO
# ---------------------------------------------------------------------------

def normalizar(texto: str) -> str:
    """Remove sufixos entre parênteses, acentos e converte para minúsculas."""
    texto = re.sub(r'\s*\(.*?\)', '', texto)   # remove (Posto 2), (RJ), etc.
    texto = re.sub(r'\s*-\s*.*$',  '', texto)  # remove " - alguma coisa"
    texto = texto.strip().lower()

    # Tabela de acentos mais comuns
    subs = str.maketrans(
        "áàãâäéèêëíìîïóòõôöúùûüç",
        "aaaaaaeeeeiiiiooooouuuuc"
    )
    return texto.translate(subs)


# Apelidos: chave = nome normalizado que vem do scraper,
#           valor = nome canônico que está no extrator
APELIDOS = {
    "recreio dos bandeirantes": "recreio",
    "barra":                    "barra da tijuca",
    "praia de ipanema":         "ipanema",
    "praia de copacabana":      "copacabana",
    "praia do leblon":          "leblon",
    "praia da barra da tijuca": "barra da tijuca",
}

def canonico(nome: str) -> str:
    n = normalizar(nome)
    return APELIDOS.get(n, n)


# ---------------------------------------------------------------------------
# SCORE
# ---------------------------------------------------------------------------

def calcular_score(onda, vento, agitacao, bal):
    score = 0

    if bal == "propria":
        score += 100
    elif bal == "impropria":
        score -= 100

    if onda is not None:
        if onda < 0.5:   score += 40
        elif onda < 1.0: score += 30
        elif onda < 1.5: score += 10
        else:            score -= 10

    if vento is not None:
        if vento < 10:   score += 30
        elif vento < 20: score += 15
        elif vento > 30: score -= 20

    # Penalidade extra por agitação forte
    if agitacao == "Forte":
        score -= 20
    elif agitacao == "Moderado":
        score -= 5

    return score


# ---------------------------------------------------------------------------
# 1. COLETAR ONDAS
# ---------------------------------------------------------------------------

print("=" * 55)
print("1. Coletando ondas (CPTEC/INPE)...")
print("=" * 55)

ondas_lista = extrator_ondasZSul.main()

if not ondas_lista:
    print("⚠️  Nenhum dado de ondas retornado.")
    ondas_lista = []

# Índice pelo nome canônico (já vem limpo do extrator)
ondas_dict = { canonico(o["nome"]): o for o in ondas_lista }

print(f"\n[MERGE] ondas_dict: {len(ondas_dict)} entradas")
print(f"[MERGE] chaves: {list(ondas_dict.keys())}")


# ---------------------------------------------------------------------------
# 2. COLETAR BALNEABILIDADE
# ---------------------------------------------------------------------------

print("\n" + "=" * 55)
print("2. Coletando balneabilidade (praialimpa.net)...")
print("=" * 55)

bal_lista = scrape_balneabilidade()
print(f"✅ {len(bal_lista)} registros coletados")
print(f"[MERGE] Amostra: {bal_lista[:3]}")

# Índice pelo nome canônico
bal_dict = {}
for item in bal_lista:
    chave = canonico(item["praia"])
    # Se a praia já existe e uma das entradas é imprópria, mantém imprópria
    if chave in bal_dict:
        if item["status"] == "impropria":
            bal_dict[chave]["status"] = "impropria"
    else:
        bal_dict[chave] = {
            "status":    item["status"],
            "regiao":    item.get("regiao"),
        }

print(f"[MERGE] bal_dict: {len(bal_dict)} entradas após deduplicação")
print(f"[MERGE] chaves: {list(bal_dict.keys())[:8]}")


# ---------------------------------------------------------------------------
# 3. MERGE: itera sobre as praias do extrator (fonte autoritativa)
# ---------------------------------------------------------------------------

print("\n" + "=" * 55)
print("3. Realizando merge...")
print("=" * 55)

dados_finais = []

for onda_item in ondas_lista:
    nome      = onda_item["nome"]          # nome canônico, ex: "Copacabana"
    chave     = canonico(nome)
    lat       = onda_item.get("lat")
    lon       = onda_item.get("lon")
    onda      = onda_item.get("onda")
    vento     = onda_item.get("vento")
    agitacao  = onda_item.get("agitacao")
    direcao   = onda_item.get("direcao")

    bal_data  = bal_dict.get(chave)

    if bal_data:
        status = bal_data["status"]
        regiao = bal_data["regiao"]
        print(f"[MERGE] ✅ '{nome}' ({chave}) → bal={status} | "
              f"onda={onda}m | vento={vento}km/h")
    else:
        status = None
        regiao = None
        print(f"[MERGE] ⚠️  '{nome}' ({chave}) → sem balneabilidade | "
              f"onda={onda}m | vento={vento}km/h")

    score = calcular_score(onda, vento, agitacao, status)

    dados_finais.append({
        "nome":           nome,
        "lat":            lat,
        "lon":            lon,
        "onda":           onda,
        "vento":          vento,
        "agitacao":       agitacao,
        "direcao":        direcao,
        "balneabilidade": status,
        "regiao":         regiao,
        "score":          score,
    })

# Praias da balneabilidade que não estão no extrator (sem ondas, mas têm coord?)
nomes_extrator = { canonico(o["nome"]) for o in ondas_lista }
extras = [k for k in bal_dict if k not in nomes_extrator]
if extras:
    print(f"\n[MERGE] ℹ️  {len(extras)} praias só na balneabilidade (sem ondas): {extras}")


# ---------------------------------------------------------------------------
# 4. ORDENAR E RECOMENDAR
# ---------------------------------------------------------------------------

dados_finais.sort(key=lambda x: x["score"], reverse=True)

proprias = [p for p in dados_finais if p["balneabilidade"] == "propria"]
melhor   = proprias[0] if proprias else (dados_finais[0] if dados_finais else {"nome": None})

print(f"\n[MERGE] Resumo final:")
print(f"         Total de praias : {len(dados_finais)}")
print(f"         Com ondas       : {sum(1 for p in dados_finais if p['onda'] is not None)}")
print(f"         Com bal.        : {sum(1 for p in dados_finais if p['balneabilidade'])}")
print(f"         Próprias        : {sum(1 for p in dados_finais if p['balneabilidade'] == 'propria')}")
print(f"         Impróprias      : {sum(1 for p in dados_finais if p['balneabilidade'] == 'impropria')}")


# ---------------------------------------------------------------------------
# 5. SALVAR JSON
# ---------------------------------------------------------------------------

json_final = {
    "ultima_atualizacao":   datetime.now().isoformat(),
    "fonte_ondas":          "CPTEC/INPE",
    "fonte_balneabilidade": "praialimpa.net",
    "praia_recomendada":    melhor["nome"],
    "praias":               dados_finais,
}

with open("praias_rj.json", "w", encoding="utf-8") as f:
    json.dump(json_final, f, indent=2, ensure_ascii=False)

print("\n✅ JSON atualizado com sucesso!")
print(f"🏖️  Praia recomendada hoje: {melhor['nome']}")
