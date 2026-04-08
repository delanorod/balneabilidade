import requests
from bs4 import BeautifulSoup

URL = "https://praialimpa.net/"

def scrape_balneabilidade():
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(URL, headers=headers, timeout=10)

        soup = BeautifulSoup(response.text, "html.parser")

        elementos = soup.find_all(string=True)

        praias = []
        status_atual = None
        nome_atual = None

        for texto in elementos:
            t = texto.strip()

            if not t:
                continue

            # identifica status
            if t == "Própria":
                status_atual = "propria"
                continue

            elif t == "Imprópria":
                status_atual = "impropria"
                continue

            # ignora títulos
            if t.lower() in ["rio de janeiro", "cidade ou praia"]:
                continue

            # se tem status definido, esse texto é nome da praia
            if status_atual:
                nome_atual = t

                praias.append({
                    "praia": nome_atual,
                    "status": status_atual
                })

                # reset (importante)
                status_atual = None

        # remove duplicados
        # unique = {}
        # for p in praias:
        #     nome = p["praia"]
        #     if nome not in unique:
        #         unique[nome] = p
        #
        # return list(unique.values())

    except Exception as e:
        print("Erro scraping Praia Limpa:", e)
        return []

if __name__ == "__main__":
    print(scrape_balneabilidade()[:5])