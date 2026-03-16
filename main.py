import os
import sys
from fastapi import FastAPI

# Garante que o diretório atual está no PATH para evitar erros de importação
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importa seus scripts (use o nome exato do arquivo sem o .py)
try:
    import extrator_ondasZSul as ondas
    from inea_scraper import INEAScraper
except ImportError as e:
    print(f"Erro ao importar scripts: {e}")
    raise e

app = FastAPI()

@app.get("/ondas")
def get_ondas():
    # Chama a função main do seu script de ondas
    # forecast_days=1 para economizar memória no plano gratuito do Render
    dados = ondas.main(horas_exibicao=12, forecast_days=1, exportar=False)
    
    resultado = {}
    for praia, df in dados.items():
        # Converte o DataFrame para um formato que o JSON aceite
        resultado[praia] = df.to_dict(orient="records")
    return resultado

@app.get("/balneabilidade")
def get_balneabilidade():
    scraper = INEAScraper()
    dados = scraper.scrape_balneabilidade()
    # O seu scraper já retorna uma lista de dataclasses/dicts, o FastAPI resolve o resto
    return dados

@app.get("/")
def home():
    return {"status": "Online", "endpoints": ["/ondas", "/balneabilidade"]}
