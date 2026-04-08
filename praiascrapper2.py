import requests
from bs4 import BeautifulSoup
from collections import defaultdict

URL = "https://praialimpa.net/"

def scrape_balneabilidade():
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(URL, headers=headers, timeout=10)

        soup = BeautifulSoup(response.text, "html.parser")
        elementos = soup.find_all(string=True)

        praias = []

        status_atual = None
        regiao_atual = None

        for texto in elementos:
            t = texto.strip()

            if not t:
                continue

            # 🌍 Detecta região
            if t in ["Rio de Janeiro", "Niterói", "Baixada Fluminense"]:
                regiao_atual = t
                continue

            # 🟢 status
            if t == "Própria":
                status_atual = "propria"
                continue

            elif t == "Imprópria":
                status_atual = "impropria"
                continue

            # ignora lixo
            if t.lower() in ["cidade ou praia"]:
                continue

            # 🏖️ praia
            if status_atual:
                praias.append({
                    "praia": t,
                    "status": status_atual,
                    "regiao": regiao_atual
                })

                status_atual = None

        # 🔥 AGRUPAMENTO POR PRAIA + REGIÃO
        agrupado = defaultdict(list)

        for p in praias:
            chave = (p["praia"], p["regiao"])
            agrupado[chave].append(p["status"])

        resultado = []

        for (praia, regiao), statuses in agrupado.items():
            if "impropria" in statuses:
                final = "impropria"
            else:
                final = "propria"

            resultado.append({
                "praia": praia,
                "status": final,
                "regiao": regiao
            })

        return resultado

    except Exception as e:
        print("Erro scraping Praia Limpa:", e)
        return []

if __name__ == "__main__":
    print(scrape_balneabilidade()[:5])