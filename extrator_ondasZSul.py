# -*- coding: utf-8 -*-
"""
Extrator de dados de ondas via API XML do CPTEC/INPE.

Endpoints utilizados:
  Busca de cidade : http://servicos.cptec.inpe.br/XML/listaCidades?city=<nome>
  Ondas dia atual : http://servicos.cptec.inpe.br/XML/cidade/<id>/dia/0/ondas.xml
  Ondas 6 dias    : http://servicos.cptec.inpe.br/XML/cidade/<id>/todos/tempos/ondas.xml

O CPTEC trabalha com o município como granularidade mínima, não com praia.
Por isso usamos um dicionário fixo que mapeia cada praia ao município CPTEC
mais representativo, mais as coordenadas reais da praia para uso externo.
"""

import requests
import xml.etree.ElementTree as ET
import json
import time
from datetime import datetime

BASE_URL = "http://servicos.cptec.inpe.br/XML"

# ---------------------------------------------------------------------------
# Mapeamento: praia → (município para busca no CPTEC, lat, lon)
# Praias que pertencem ao mesmo município compartilham o mesmo ID CPTEC,
# mas guardam coordenadas individuais para uso no app.
# ---------------------------------------------------------------------------
PRAIAS = [
    {"nome": "Copacabana",      "cidade_cptec": "Rio de Janeiro", "lat": -22.9711, "lon": -43.1822},
    {"nome": "Arpoador",        "cidade_cptec": "Rio de Janeiro", "lat": -22.9870, "lon": -43.1910},
    {"nome": "Ipanema",         "cidade_cptec": "Rio de Janeiro", "lat": -22.9836, "lon": -43.2045},
    {"nome": "Leblon",          "cidade_cptec": "Rio de Janeiro", "lat": -22.9896, "lon": -43.2249},
    {"nome": "Barra da Tijuca", "cidade_cptec": "Rio de Janeiro", "lat": -23.0016, "lon": -43.3659},
    {"nome": "Recreio",         "cidade_cptec": "Rio de Janeiro", "lat": -23.0293, "lon": -43.4800},
    {"nome": "Grumari",         "cidade_cptec": "Rio de Janeiro", "lat": -23.0500, "lon": -43.5300},
    {"nome": "Prainha",         "cidade_cptec": "Rio de Janeiro", "lat": -23.0444, "lon": -43.5136},
    {"nome": "Flamengo",        "cidade_cptec": "Rio de Janeiro", "lat": -22.9300, "lon": -43.1750},
    {"nome": "Botafogo",        "cidade_cptec": "Rio de Janeiro", "lat": -22.9500, "lon": -43.1800},
    {"nome": "Itacoatiara",     "cidade_cptec": "Niterói",        "lat": -22.9667, "lon": -43.0333},
    {"nome": "Camboinhas",      "cidade_cptec": "Niterói",        "lat": -22.9600, "lon": -43.0600},
    {"nome": "Icaraí",          "cidade_cptec": "Niterói",        "lat": -22.9000, "lon": -43.1167},
]


# ---------------------------------------------------------------------------
# Cache de IDs para não repetir buscas do mesmo município
# ---------------------------------------------------------------------------
_cache_ids: dict[str, str] = {}


def buscar_id_cidade(nome_cidade: str) -> str | None:
    """
    Busca o ID numérico de uma cidade no CPTEC.
    Retorna o ID como string ou None se não encontrar.
    """
    if nome_cidade in _cache_ids:
        print(f"[DEBUG]   (cache) '{nome_cidade}' → id={_cache_ids[nome_cidade]}")
        return _cache_ids[nome_cidade]

    url = f"{BASE_URL}/listaCidades"
    params = {"city": nome_cidade}

    print(f"[DEBUG] Buscando ID para '{nome_cidade}' em {url}")
    try:
        resp = requests.get(url, params=params, timeout=10)
        print(f"[DEBUG]   status HTTP: {resp.status_code}")

        root = ET.fromstring(resp.content)
        cidades = root.findall("cidade")
        print(f"[DEBUG]   cidades encontradas: {len(cidades)}")

        for cidade in cidades:
            nome_ret = (cidade.findtext("nome") or "").strip()
            uf_ret   = (cidade.findtext("uf")   or "").strip()
            id_ret   = (cidade.findtext("id")   or "").strip()
            print(f"[DEBUG]     → '{nome_ret}' ({uf_ret}) id={id_ret}")

            # Prioriza match exato + UF RJ ou ES (municípios litorâneos relevantes)
            if nome_ret.lower() == nome_cidade.lower():
                _cache_ids[nome_cidade] = id_ret
                print(f"[DEBUG]   ✅ Match exato: id={id_ret}")
                return id_ret

        # Sem match exato: usa o primeiro resultado
        if cidades:
            id_ret = (cidades[0].findtext("id") or "").strip()
            _cache_ids[nome_cidade] = id_ret
            print(f"[DEBUG]   ⚠️ Sem match exato, usando primeiro: id={id_ret}")
            return id_ret

        print(f"[DEBUG]   ❌ Nenhuma cidade encontrada para '{nome_cidade}'")
        return None

    except Exception as e:
        print(f"[DEBUG]   ❌ Erro ao buscar cidade '{nome_cidade}': {e}")
        return None


def buscar_ondas_cptec(cidade_id: str) -> dict:
    """
    Busca a previsão de ondas do dia atual para um ID de cidade no CPTEC.
    Retorna um dict com onda (média manhã/tarde/noite), vento, agitacao e direcao.
    """
    url = f"{BASE_URL}/cidade/{cidade_id}/dia/0/ondas.xml"
    print(f"[DEBUG]   Buscando ondas: {url}")

    try:
        resp = requests.get(url, timeout=10)
        print(f"[DEBUG]   status HTTP: {resp.status_code}")

        root = ET.fromstring(resp.content)

        alturas = []
        ventos  = []
        agitacoes = []
        direcoes  = []

        for periodo in ["manha", "tarde", "noite"]:
            elem = root.find(periodo)
            if elem is None:
                continue

            altura   = elem.findtext("altura")
            vento    = elem.findtext("vento")
            agitacao = elem.findtext("agitacao")
            direcao  = elem.findtext("direcao")

            print(f"[DEBUG]     {periodo}: altura={altura}m | vento={vento}km/h "
                  f"| agitacao={agitacao} | direcao={direcao}")

            if altura:
                try:
                    alturas.append(float(altura))
                except ValueError:
                    pass
            if vento:
                try:
                    ventos.append(float(vento))
                except ValueError:
                    pass
            if agitacao:
                agitacoes.append(agitacao)
            if direcao:
                direcoes.append(direcao)

        onda_media  = round(sum(alturas) / len(alturas), 2) if alturas else None
        vento_medio = round(sum(ventos)  / len(ventos),  2) if ventos  else None

        # Agitação: se houver Forte no dia, reporta Forte; senão Moderado, senão Fraco
        if "Forte" in agitacoes:
            agitacao_final = "Forte"
        elif "Moderado" in agitacoes:
            agitacao_final = "Moderado"
        elif agitacoes:
            agitacao_final = agitacoes[0]
        else:
            agitacao_final = None

        direcao_final = direcoes[1] if len(direcoes) > 1 else (direcoes[0] if direcoes else None)

        print(f"[DEBUG]   ✅ Resumo: onda={onda_media}m | vento={vento_medio}km/h "
              f"| agitacao={agitacao_final} | direcao={direcao_final}")

        return {
            "onda":     onda_media,
            "vento":    vento_medio,
            "agitacao": agitacao_final,
            "direcao":  direcao_final,
        }

    except Exception as e:
        print(f"[DEBUG]   ❌ Erro ao buscar ondas (id={cidade_id}): {e}")
        return {"onda": None, "vento": None, "agitacao": None, "direcao": None}


def calcular_score(onda, vento):
    score = 0
    if onda is not None:
        if 0.5 <= onda <= 1.5:
            score += 5
    if vento is not None:
        if vento < 15:
            score += 5
    return score


def main():
    print(f"\n{'='*60}")
    print(f"[DEBUG] Extrator CPTEC/INPE iniciado: {datetime.now().isoformat()}")
    print(f"{'='*60}\n")

    resultado = []

    for praia in PRAIAS:
        nome         = praia["nome"]
        cidade_cptec = praia["cidade_cptec"]
        lat          = praia["lat"]
        lon          = praia["lon"]

        print(f"\n[DEBUG] ── Praia: {nome} (via município '{cidade_cptec}')")

        cidade_id = buscar_id_cidade(cidade_cptec)

        if not cidade_id:
            print(f"[DEBUG]   ⚠️ ID não encontrado, praia ignorada.")
            continue

        dados = buscar_ondas_cptec(cidade_id)

        onda  = dados.get("onda")
        vento = dados.get("vento")
        score = calcular_score(onda, vento)

        resultado.append({
            "nome":     nome,
            "lat":      lat,
            "lon":      lon,
            "onda":     onda,
            "vento":    vento,
            "agitacao": dados.get("agitacao"),
            "direcao":  dados.get("direcao"),
            "score":    score,
        })

        time.sleep(0.5)  # respeita o servidor do INPE

    print(f"\n{'='*60}")
    print(f"[DEBUG] Coleta concluída: {len(resultado)} praias")
    print(f"[DEBUG] Amostra (3 primeiras):")
    for p in resultado[:3]:
        print(f"         {p}")
    print(f"{'='*60}\n")

    with open("praias_rj.json", "w", encoding="utf-8") as f:
        json.dump({
            "ultima_atualizacao": datetime.utcnow().isoformat(),
            "fonte": "CPTEC/INPE",
            "praias": resultado
        }, f, ensure_ascii=False, indent=2)

    print("[DEBUG] ✅ Arquivo praias_rj.json salvo.")
    return resultado


if __name__ == "__main__":
    main()
