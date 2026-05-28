import requests
from bs4 import BeautifulSoup, NavigableString, Tag
from collections import defaultdict
from datetime import datetime

URL = "https://praialimpa.net/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

# Status reconhecidos (lowercase para comparação)
STATUS_MAP = {
    "própria": "propria",
    "imprópria": "impropria",
    "n/a": "nao_disponivel",
}

# Textos de interface que devem ser ignorados
TEXTOS_IGNORADOS = {
    "cidade ou praia",
    "praialimpa.net",
    "fonte",
    "observação",
    "praias limpas do rio de janeiro",
}


def limpar_texto(t: str) -> str:
    return " ".join(t.split())


def scrape_balneabilidade(url: str = URL) -> list[dict]:
    """
    Coleta dados de balneabilidade do praialimpa.net.

    Retorna lista de dicts com campos:
        praia, ponto, status, regiao, atualizado_em
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"[ERRO] Falha ao acessar {url}: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    # Remove ruído: scripts, estilos, navegação e rodapé
    for tag in soup(["script", "style", "noscript", "nav", "footer", "head"]):
        tag.decompose()

    # Coleta regiões via <h1> para detecção dinâmica
    regioes_h1 = {h1.get_text(strip=True) for h1 in soup.find_all("h1")}

    # Coleta todos os nós de texto em ordem de documento
    textos = [
        limpar_texto(node)
        for node in soup.find_all(string=True)
        if isinstance(node, NavigableString) and limpar_texto(node)
    ]

    # ── Máquina de estados ────────────────────────────────────────────────────
    # Ciclo esperado:  [REGIÃO?] → STATUS → PRAIA → PONTO → repete
    #
    # O ponto (localização) vem logo após o nome da praia e é capturado
    # separadamente. Se não existir (ex: próximo item já é um status),
    # fica como string vazia.

    praias = []

    regiao_atual = "Desconhecida"
    atualizado_em = ""
    status_atual = None
    praia_atual = None

    # Estados possíveis do parser
    # "aguardando_status"  → esperando "Própria" / "Imprópria" / "n/a"
    # "aguardando_praia"   → status definido, próximo texto = nome da praia
    # "aguardando_ponto"   → praia definida, próximo texto = ponto/localização

    estado = "aguardando_status"

    for t in textos:
        t_lower = t.lower()

        # ── Detecta região (h1) ───────────────────────────────────────────────
        if t in regioes_h1:
            regiao_atual = t
            estado = "aguardando_status"
            status_atual = None
            praia_atual = None
            continue

        # ── Detecta data de atualização ───────────────────────────────────────
        if t_lower.startswith("atualizado em"):
            atualizado_em = t
            continue

        # ── Ignora textos de interface ────────────────────────────────────────
        if t_lower in TEXTOS_IGNORADOS:
            continue

        # ── Detecta status ────────────────────────────────────────────────────
        if t_lower in STATUS_MAP:
            # Se havia uma praia sem ponto ainda, finaliza ela sem ponto
            if estado == "aguardando_ponto" and praia_atual:
                praias.append({**praia_atual, "ponto": ""})
                praia_atual = None

            status_atual = STATUS_MAP[t_lower]
            estado = "aguardando_praia"
            continue

        # ── Captura nome da praia ─────────────────────────────────────────────
        if estado == "aguardando_praia" and status_atual:
            praia_atual = {
                "praia": t,
                "status": status_atual,
                "regiao": regiao_atual,
                "atualizado_em": atualizado_em,
            }
            estado = "aguardando_ponto"
            continue

        # ── Captura ponto/localização ─────────────────────────────────────────
        if estado == "aguardando_ponto" and praia_atual:
            praias.append({**praia_atual, "ponto": t})
            praia_atual = None
            status_atual = None
            estado = "aguardando_status"
            continue

    # Finaliza última praia pendente (caso o HTML termine sem ponto)
    if praia_atual:
        praias.append({**praia_atual, "ponto": ""})

    # ── Agrupamento por (praia, regiao) ───────────────────────────────────────
    # Regra: se qualquer ponto da praia for impróprio → praia = imprópria
    # Pontos com status "nao_disponivel" são mantidos individualmente.

    agrupado: dict[tuple, dict] = {}

    for p in praias:
        if p["status"] == "nao_disponivel":
            # Mantém pontos n/a sem interferir no agrupamento
            praias_finais_na = agrupado.setdefault(
                (p["praia"], p["regiao"], "nao_disponivel"), p
            )
            continue

        chave = (p["praia"], p["regiao"])
        if chave not in agrupado:
            agrupado[chave] = {**p, "pontos": []}

        agrupado[chave]["pontos"].append(
            {"ponto": p["ponto"], "status": p["status"]}
        )

        # Praia imprópria se qualquer ponto for impróprio
        if p["status"] == "impropria":
            agrupado[chave]["status"] = "impropria"
        elif agrupado[chave]["status"] != "impropria":
            agrupado[chave]["status"] = "propria"

    resultado = list(agrupado.values())

    return resultado


def resumo(resultado: list[dict]) -> None:
    """Imprime um resumo amigável dos dados coletados."""
    total = len(resultado)
    proprias = sum(1 for p in resultado if p["status"] == "propria")
    improprias = sum(1 for p in resultado if p["status"] == "impropria")
    nd = sum(1 for p in resultado if p["status"] == "nao_disponivel")

    print(f"\n{'='*50}")
    print(f"  Total de praias: {total}")
    print(f"  ✅ Próprias    : {proprias}")
    print(f"  ❌ Impróprias  : {improprias}")
    print(f"  ❓ N/A         : {nd}")
    print(f"{'='*50}\n")

    regioes = sorted({p["regiao"] for p in resultado})
    for regiao in regioes:
        praias_regiao = [p for p in resultado if p["regiao"] == regiao]
        prop = sum(1 for p in praias_regiao if p["status"] == "propria")
        imp = sum(1 for p in praias_regiao if p["status"] == "impropria")
        print(f"  {regiao}: {len(praias_regiao)} praias  ✅{prop} ❌{imp}")


if __name__ == "__main__":
    print(f"Coletando dados de {URL} ...")
    dados = scrape_balneabilidade()

    if not dados:
        print("[AVISO] Nenhum dado coletado. Verifique sua conexão ou o site.")
    else:
        resumo(dados)
        print("\nPrimeiras 5 entradas:")
        for p in dados[:5]:
            pontos_str = ""
            if "pontos" in p:
                pontos_str = f" ({len(p['pontos'])} pontos)"
            print(
                f"  [{p['regiao']}] {p['praia']}{pontos_str} → {p['status']}"
            )
