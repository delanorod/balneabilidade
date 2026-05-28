# -*- coding: utf-8 -*-
"""
Gera praias_rj.json combinando:
  - Ondas/vento     : extrator_ondasZSul (Open-Meteo)
  - Balneabilidade  : inea_scraper2.INEAScraper
                      (praialimpa.net → fallback PDF INEA)

Estratégia de merge:
  A fonte autoritativa de nomes e coordenadas é o cadastro interno do
  INEAScraper (INEAScraper.PRAIAS). Isso evita dependência circular com
  o praias_rj.json ainda não gerado na primeira execução.
"""

import json
import unicodedata
from datetime import datetime

from extrator_ondasZSul import extrair_dados
from inea_scraper2 import INEAScraper


# ---------------------------------------------------------------------------
# NORMALIZAÇÃO
# ---------------------------------------------------------------------------

def normalizar_nome(nome: str) -> str:
    nome = nome.lower().strip()
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('ASCII')
    nome = nome.split(" - ")[0]
    return nome


APELIDOS = {
    "recreio dos bandeirantes": "recreio",
    "barra":                    "barra da tijuca",
    "praia de ipanema":         "ipanema",
    "praia de copacabana":      "copacabana",
    "praia do leblon":          "leblon",
    "praia da barra da tijuca": "barra da tijuca",
}


def canonico(nome: str) -> str:
    n = normalizar_nome(nome)
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

    if agitacao == "Forte":
        score -= 20
    elif agitacao == "Moderado":
        score -= 5

    return score


# ---------------------------------------------------------------------------
# 0. MONTAR LISTA DE PRAIAS A PARTIR DO CADASTRO DO INEAScraper
#    Isso resolve a dependência circular: o extrator_ondasZSul não precisa
#    mais ler praias_rj.json para saber quais praias buscar.
# ---------------------------------------------------------------------------

print("=" * 55)
print("0. Montando lista de praias do cadastro INEAScraper...")
print("=" * 55)

praias_cadastro = [
    {
        "nome": info["nome"],
        "lat":  info["coordenadas"]["latitude"],
        "lon":  info["coordenadas"]["longitude"],
    }
    for info in INEAScraper.PRAIAS.values()
    if info.get("coordenadas")
]

print(f"   {len(praias_cadastro)} praias com coordenadas encontradas.")


# ---------------------------------------------------------------------------
# 1. COLETAR ONDAS  (passa a lista — sem ler praias_rj.json)
# ---------------------------------------------------------------------------

print("\n" + "=" * 55)
print("1. Coletando ondas (Open-Meteo)...")
print("=" * 55)

ondas_lista = extrair_dados(praias=praias_cadastro)

if not ondas_lista:
    print("⚠️  Nenhum dado de ondas retornado.")

ondas_dict = {canonico(o["nome"]): o for o in ondas_lista}

print(f"\n[MERGE] ondas_dict: {len(ondas_dict)} entradas")
print(f"[MERGE] chaves: {list(ondas_dict.keys())}")


# ---------------------------------------------------------------------------
# 2. COLETAR BALNEABILIDADE  (INEAScraper — 2 camadas)
# ---------------------------------------------------------------------------

print("\n" + "=" * 55)
print("2. Coletando balneabilidade (INEAScraper v3.0)...")
print("=" * 55)

try:
    scraper = INEAScraper()
    bal_objetos = scraper.scrape_balneabilidade()
except RuntimeError as e:
    print(f"❌ INEAScraper falhou: {e}")
    bal_objetos = []

print(f"✅ {len(bal_objetos)} registros coletados")

# bal_dict: canonico(nome) → BalneabilidadeData  (imprópria tem prioridade)
bal_dict: dict = {}
for obj in bal_objetos:
    chave = canonico(obj.praia_nome)
    if chave in bal_dict:
        if obj.status == "impropria":
            bal_dict[chave].status = "impropria"
    else:
        bal_dict[chave] = obj

print(f"[MERGE] bal_dict: {len(bal_dict)} entradas após deduplicação")
print(f"[MERGE] amostra de chaves: {list(bal_dict.keys())[:8]}")


# ---------------------------------------------------------------------------
# 3. MERGE — itera sobre o cadastro (fonte canônica de nomes/coords)
# ---------------------------------------------------------------------------

print("\n" + "=" * 55)
print("3. Realizando merge...")
print("=" * 55)

dados_finais = []

for info_praia in praias_cadastro:
    nome  = info_praia["nome"]
    chave = canonico(nome)
    lat   = info_praia["lat"]
    lon   = info_praia["lon"]

    onda_item = ondas_dict.get(chave, {})
    onda      = onda_item.get("onda")
    vento     = onda_item.get("vento")
    agitacao  = onda_item.get("agitacao")
    direcao   = onda_item.get("direcao")

    bal_obj = bal_dict.get(chave)

    if bal_obj:
        status          = bal_obj.status
        regiao          = bal_obj.regiao
        municipio       = bal_obj.municipio
        bairro          = bal_obj.bairro
        coliformes      = bal_obj.coliformes_fecais
        observacoes     = bal_obj.observacoes
        data_coleta     = bal_obj.data_coleta
        fonte_bal       = bal_obj.fonte
        caracteristicas = bal_obj.caracteristicas
        extensao_km     = bal_obj.extensao_km
        print(f"[MERGE] ✅ '{nome}' ({chave}) → bal={status} | "
              f"onda={onda}m | vento={vento}km/h | fonte={fonte_bal}")
    else:
        # Dados do cadastro estático como fallback
        cadastro_info   = INEAScraper.PRAIAS.get(chave, {})
        status          = None
        regiao          = cadastro_info.get("regiao")
        municipio       = cadastro_info.get("municipio")
        bairro          = cadastro_info.get("bairro", "")
        coliformes      = None
        observacoes     = None
        data_coleta     = None
        fonte_bal       = None
        caracteristicas = cadastro_info.get("caracteristicas", [])
        extensao_km     = cadastro_info.get("extensao_km")
        print(f"[MERGE] ⚠️  '{nome}' ({chave}) → sem balneabilidade | "
              f"onda={onda}m | vento={vento}km/h")

    score = calcular_score(onda, vento, agitacao, status)

    dados_finais.append({
        "nome":             nome,
        "lat":              lat,
        "lon":              lon,
        "onda":             onda,
        "vento":            vento,
        "agitacao":         agitacao,
        "direcao":          direcao,
        "balneabilidade":   status,
        "regiao":           regiao,
        "municipio":        municipio,
        "bairro":           bairro,
        "extensao_km":      extensao_km,
        "caracteristicas":  caracteristicas,
        "coliformes_fecais": coliformes,
        "observacoes":      observacoes,
        "data_coleta":      data_coleta,
        "fonte_bal":        fonte_bal,
        "score":            score,
    })

# Praias com balneabilidade mas fora do cadastro (regiões extras)
nomes_cadastro = {canonico(p["nome"]) for p in praias_cadastro}
extras = [k for k in bal_dict if k not in nomes_cadastro]
if extras:
    print(f"\n[MERGE] ℹ️  {len(extras)} praias só na balneabilidade (sem coords): {extras}")


# ---------------------------------------------------------------------------
# 4. ORDENAR E RECOMENDAR
# ---------------------------------------------------------------------------

dados_finais.sort(key=lambda x: x["score"], reverse=True)

proprias = [p for p in dados_finais if p["balneabilidade"] == "propria"]
melhor   = proprias[0] if proprias else (dados_finais[0] if dados_finais else {"nome": None})

fonte_usada = bal_objetos[0].fonte if bal_objetos else "n/a"

print(f"\n[MERGE] Resumo final:")
print(f"  Total de praias  : {len(dados_finais)}")
print(f"  Com ondas        : {sum(1 for p in dados_finais if p['onda'] is not None)}")
print(f"  Com bal.         : {sum(1 for p in dados_finais if p['balneabilidade'])}")
print(f"  Próprias         : {sum(1 for p in dados_finais if p['balneabilidade'] == 'propria')}")
print(f"  Impróprias       : {sum(1 for p in dados_finais if p['balneabilidade'] == 'impropria')}")
print(f"  Fonte bal usada  : {fonte_usada}")


# ---------------------------------------------------------------------------
# 5. SALVAR JSON
# ---------------------------------------------------------------------------

json_final = {
    "ultima_atualizacao":   datetime.now().isoformat(),
    "fonte_ondas":          "Open-Meteo",
    "fonte_balneabilidade": fonte_usada,
    "praia_recomendada":    melhor["nome"],
    "praias":               dados_finais,
}

with open("praias_rj.json", "w", encoding="utf-8") as f:
    json.dump(json_final, f, indent=2, ensure_ascii=False)

print("\n✅ JSON atualizado com sucesso!")
print(f"🏖️  Praia recomendada hoje: {melhor['nome']}")
