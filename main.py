import time
from fastapi import FastAPI
import extrator_ondasZSul as ondas
from inea_scraper import INEAScraper

app = FastAPI()

# Configurações de Cache (em segundos)
CACHE_DURATION = 1800  # 30 minutos
cache_storage = {
    "ondas": {"data": None, "timestamp": 0},
    "balneabilidade": {"data": None, "timestamp": 0}
}

@app.get("/ondas")
def get_ondas():
    agora = time.time()
    # Verifica se o cache ainda é válido
    if cache_storage["ondas"]["data"] and (agora - cache_storage["ondas"]["timestamp"] < CACHE_DURATION):
        return cache_storage["ondas"]["data"]

    # Se não houver cache ou expirou, executa o script
    dados = ondas.main(horas_exibicao=12, forecast_days=1, exportar=False)
    resultado = {praia: df.to_dict(orient="records") for praia, df in dados.items()}
    
    # Atualiza o cache
    cache_storage["ondas"] = {"data": resultado, "timestamp": agora}
    return resultado

@app.get("/balneabilidade")
def get_balneabilidade():
    agora = time.time()
    if cache_storage["balneabilidade"]["data"] and (agora - cache_storage["balneabilidade"]["timestamp"] < CACHE_DURATION):
        return cache_storage["balneabilidade"]["data"]

    scraper = INEAScraper()
    resultado = scraper.scrape_balneabilidade()
    
    cache_storage["balneabilidade"] = {"data": resultado, "timestamp": agora}
    return resultado
