from fastapi import FastAPI
import extrator_ondasZSul as ondas
from inea_scraper import INEAScraper

app = FastAPI()

@app.get("/ondas")
def get_ondas():
    # Chama a função main do seu script de ondas
    # forecast_days=1 para ser mais rápido no plano gratuito
    dados = ondas.main(horas_exibicao=12, forecast_days=1, exportar=False)
    
    # Transformando o dicionário de DataFrames em JSON serializável
    resultado = {}
    for praia, df in dados.items():
        resultado[praia] = df.to_dict(orient="records")
    return resultado

@app.get("/balneabilidade")
def get_balneabilidade():
    scraper = INEAScraper()
    # Coleta os dados (reais ou mockados dependendo do site do INEA)
    dados = scraper.scrape_balneabilidade()
    return dados

@app.get("/")
def home():
    return {"status": "Online", "endpoints": ["/ondas", "/balneabilidade"]}
