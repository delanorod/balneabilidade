import xarray as xr
import pandas as pd
import requests
import os
from datetime import datetime, timedelta

# Seu dicionário de praias
PRAIAS = {
        # Zona Sul
        "leme":         {"nome": "Leme",         "municipio": "Rio de Janeiro", "bairro": "Leme",         "regiao": "Zona Sul",   "coordenadas": {"latitude": -22.9635, "longitude": -43.1674}, "extensao_km": 0.8,  "caracteristicas": ["urbanizada","familiar"]},
        "copacabana":   {"nome": "Copacabana",   "municipio": "Rio de Janeiro", "bairro": "Copacabana",   "regiao": "Zona Sul",   "coordenadas": {"latitude": -22.9711, "longitude": -43.1823}, "extensao_km": 4.0,  "caracteristicas": ["urbanizada","turística"]},
        "arpoador":     {"nome": "Arpoador",     "municipio": "Rio de Janeiro", "bairro": "Ipanema",      "regiao": "Zona Sul",   "coordenadas": {"latitude": -22.9876, "longitude": -43.1940}, "extensao_km": 0.5,  "caracteristicas": ["surf","pôr-do-sol"]},
        "ipanema":      {"nome": "Ipanema",      "municipio": "Rio de Janeiro", "bairro": "Ipanema",      "regiao": "Zona Sul",   "coordenadas": {"latitude": -22.9868, "longitude": -43.2040}, "extensao_km": 2.5,  "caracteristicas": ["urbanizada","turística"]},
        "leblon":       {"nome": "Leblon",       "municipio": "Rio de Janeiro", "bairro": "Leblon",       "regiao": "Zona Sul",   "coordenadas": {"latitude": -22.9877, "longitude": -43.2230}, "extensao_km": 1.5,  "caracteristicas": ["urbanizada","nobre"]},
        "vidigal":      {"nome": "Vidigal",      "municipio": "Rio de Janeiro", "bairro": "Vidigal",      "regiao": "Zona Sul",   "coordenadas": {"latitude": -22.9932, "longitude": -43.2349}, "extensao_km": 0.4,  "caracteristicas": ["pequena","pitoresca"]},
        "sao_conrado":  {"nome": "São Conrado",  "municipio": "Rio de Janeiro", "bairro": "São Conrado",  "regiao": "Zona Sul",   "coordenadas": {"latitude": -23.0038, "longitude": -43.2753}, "extensao_km": 1.7,  "caracteristicas": ["surf","tranquila"]},
        "joatinga":     {"nome": "Joatinga",     "municipio": "Rio de Janeiro", "bairro": "Joá",          "regiao": "Zona Sul",   "coordenadas": {"latitude": -23.0102, "longitude": -43.2879}, "extensao_km": 0.5,  "caracteristicas": ["secreta","bela"]},
        "pepino":       {"nome": "Pepino",       "municipio": "Rio de Janeiro", "bairro": "São Conrado",  "regiao": "Zona Sul",   "coordenadas": {"latitude": -23.0070, "longitude": -43.2820}, "extensao_km": 0.8,  "caracteristicas": ["asa-delta","tranquila"]},
        "diabo":        {"nome": "Diabo",        "municipio": "Rio de Janeiro", "bairro": "Ipanema",      "regiao": "Zona Sul",   "coordenadas": {"latitude": -22.9880, "longitude": -43.1960}, "extensao_km": 0.1,  "caracteristicas": ["pequena","rochosa"]},
        "flamengo":     {"nome": "Flamengo",     "municipio": "Rio de Janeiro", "bairro": "Flamengo",     "regiao": "Zona Sul",   "coordenadas": {"latitude": -22.9295, "longitude": -43.1736}, "extensao_km": 1.3,  "caracteristicas": ["baía","esporte"]},
        "botafogo":     {"nome": "Botafogo",     "municipio": "Rio de Janeiro", "bairro": "Botafogo",     "regiao": "Zona Sul",   "coordenadas": {"latitude": -22.9519, "longitude": -43.1820}, "extensao_km": 0.9,  "caracteristicas": ["baía","histórica"]},
        "urca":         {"nome": "Urca",         "municipio": "Rio de Janeiro", "bairro": "Urca",         "regiao": "Zona Sul",   "coordenadas": {"latitude": -22.9486, "longitude": -43.1637}, "extensao_km": 0.4,  "caracteristicas": ["tranquila","baía"]},
        "vermelha":     {"nome": "Vermelha",     "municipio": "Rio de Janeiro", "bairro": "Urca",         "regiao": "Zona Sul",   "coordenadas": {"latitude": -22.9533, "longitude": -43.1607}, "extensao_km": 0.3,  "caracteristicas": ["pequena","mergulho"]},
        "gloria":       {"nome": "Glória",       "municipio": "Rio de Janeiro", "bairro": "Glória",       "regiao": "Zona Sul",   "coordenadas": {"latitude": -22.9232, "longitude": -43.1740}, "extensao_km": 0.5,  "caracteristicas": ["baía","histórica"]},
        # Zona Oeste
        "barra":        {"nome": "Barra da Tijuca",            "municipio": "Rio de Janeiro", "bairro": "Barra da Tijuca",            "regiao": "Zona Oeste", "coordenadas": {"latitude": -23.0048, "longitude": -43.3658}, "extensao_km": 18.0, "caracteristicas": ["surf","maior-praia"]},
        "recreio":      {"nome": "Recreio dos Bandeirantes",   "municipio": "Rio de Janeiro", "bairro": "Recreio dos Bandeirantes",   "regiao": "Zona Oeste", "coordenadas": {"latitude": -23.0241, "longitude": -43.4626}, "extensao_km": 5.0,  "caracteristicas": ["familiar","tranquila"]},
        "macumba":      {"nome": "Macumba",                    "municipio": "Rio de Janeiro", "bairro": "Recreio dos Bandeirantes",   "regiao": "Zona Oeste", "coordenadas": {"latitude": -23.0310, "longitude": -43.4921}, "extensao_km": 1.5,  "caracteristicas": ["surf","jovem"]},
        "prainha":      {"nome": "Prainha",                    "municipio": "Rio de Janeiro", "bairro": "Recreio dos Bandeirantes",   "regiao": "Zona Oeste", "coordenadas": {"latitude": -23.0415, "longitude": -43.5043}, "extensao_km": 0.7,  "caracteristicas": ["surf","preservada"]},
        "grumari":      {"nome": "Grumari",                    "municipio": "Rio de Janeiro", "bairro": "Grumari",                    "regiao": "Zona Oeste", "coordenadas": {"latitude": -23.0548, "longitude": -43.5283}, "extensao_km": 1.5,  "caracteristicas": ["APA","selvagem"]},
        "pontal":       {"nome": "Pontal de Sernambetiba",     "municipio": "Rio de Janeiro", "bairro": "Recreio dos Bandeirantes",   "regiao": "Zona Oeste", "coordenadas": {"latitude": -23.0180, "longitude": -43.4450}, "extensao_km": 2.0,  "caracteristicas": ["tranquila"]},
        "barra_guaratiba": {"nome": "Barra de Guaratiba",      "municipio": "Rio de Janeiro", "bairro": "Guaratiba",                  "regiao": "Zona Oeste", "coordenadas": {"latitude": -23.0650, "longitude": -43.5700}, "extensao_km": 1.0,  "caracteristicas": ["pesca","tranquila"]},
        # Niterói
        "icarai":       {"nome": "Icaraí",       "municipio": "Niterói", "bairro": "Icaraí",       "regiao": "Niterói", "coordenadas": {"latitude": -22.9035, "longitude": -43.1106}, "extensao_km": 1.2, "caracteristicas": ["urbanizada","familiar"]},
        "charitas":     {"nome": "Charitas",     "municipio": "Niterói", "bairro": "Charitas",     "regiao": "Niterói", "coordenadas": {"latitude": -22.9231, "longitude": -43.1200}, "extensao_km": 0.6, "caracteristicas": ["baía","calma"]},
        "jurujuba":     {"nome": "Jurujuba",     "municipio": "Niterói", "bairro": "Jurujuba",     "regiao": "Niterói", "coordenadas": {"latitude": -22.9354, "longitude": -43.1118}, "extensao_km": 0.5, "caracteristicas": ["pesca","baía"]},
        "camboinhas":   {"nome": "Camboinhas",   "municipio": "Niterói", "bairro": "Camboinhas",   "regiao": "Niterói", "coordenadas": {"latitude": -22.9645, "longitude": -43.0534}, "extensao_km": 1.0, "caracteristicas": ["tranquila"]},
        "itacoatiara":  {"nome": "Itacoatiara",  "municipio": "Niterói", "bairro": "Itacoatiara",  "regiao": "Niterói", "coordenadas": {"latitude": -22.9681, "longitude": -43.0356}, "extensao_km": 1.5, "caracteristicas": ["surf","rochosa"]},
        "itaipu":       {"nome": "Itaipu",       "municipio": "Niterói", "bairro": "Itaipu",       "regiao": "Niterói", "coordenadas": {"latitude": -22.9591, "longitude": -43.0493}, "extensao_km": 2.0, "caracteristicas": ["pesca","surf"]},
        "piratininga":  {"nome": "Piratininga",  "municipio": "Niterói", "bairro": "Piratininga",  "regiao": "Niterói", "coordenadas": {"latitude": -22.9554, "longitude": -43.0588}, "extensao_km": 1.8, "caracteristicas": ["lagoa","kite"]},
        "gragoata":     {"nome": "Gragoatá",     "municipio": "Niterói", "bairro": "São Domingos", "regiao": "Niterói", "coordenadas": {"latitude": -22.8950, "longitude": -43.1230}, "extensao_km": 0.3, "caracteristicas": ["baía","pequena"]},
        "boa_viagem":   {"nome": "Boa Viagem",   "municipio": "Niterói", "bairro": "Boa Viagem",   "regiao": "Niterói", "coordenadas": {"latitude": -22.8990, "longitude": -43.1150}, "extensao_km": 0.4, "caracteristicas": ["baía","ilha"]},
        "sao_francisco":{"nome": "São Francisco","municipio": "Niterói", "bairro": "São Francisco","regiao": "Niterói", "coordenadas": {"latitude": -22.9150, "longitude": -43.1180}, "extensao_km": 0.5, "caracteristicas": ["baía"]},
    }  # Insira aqui o dicionário completo que você forneceu

# Configurações
DATA_ALVO = (datetime.now() - timedelta(days=25)).strftime('%Y%m%d')  # Ex: 7 dias atrás
BASE_URL = "https://www.star.nesdis.noaa.gov/data/pub0015/coastwatch/blended/wind/science/uvcomp/daily/2026/"
NOME_ARQUIVO = f"NBSv02_wind_daily_{DATA_ALVO}.nc"
URL_COMPLETA = BASE_URL + NOME_ARQUIVO
DIRETORIO_DOWNLOAD = "./dados_noaa_nbsv2"
caminho_arquivo = os.path.join(DIRETORIO_DOWNLOAD, NOME_ARQUIVO)

# Cria o diretório se não existir
os.makedirs(DIRETORIO_DOWNLOAD, exist_ok=True)


# Função para baixar o arquivo
def baixar_dados(url, caminho):
    print(f"Baixando {url} ...")
    try:
        resposta = requests.get(url, stream=True)
        resposta.raise_for_status()  # Verifica se houve erro no download
        with open(caminho, 'wb') as f:
            for chunk in resposta.iter_content(chunk_size=8192):
                f.write(chunk)
        print("Download concluído!")
        return True
    except Exception as e:
        print(f"Erro no download: {e}")
        return False


# Função principal para extrair dados
def extrair_dados_praias(arquivo_nc, praias_dict):
    print(f"Abrindo arquivo: {arquivo_nc}")
    try:
        # Abre o dataset NetCDF
        ds = xr.open_dataset(arquivo_nc)

        # Acessa as variáveis de interesse (verifique os nomes exatos no arquivo)
        # Suposição: 'u_wind' e 'v_wind' ou 'uwnd' e 'vwnd'
        # Você pode precisar ajustar com base no que encontrar no arquivo
        u10 = ds['uwnd']
        v10 = ds['vwnd']
        lat = ds['latitude']
        lon = ds['longitude']

        resultados = []
        for chave, info in praias_dict.items():
            lat_alvo = info['coordenadas']['latitude']
            lon_alvo = info['coordenadas']['longitude']

            # Seleciona o ponto de grade mais próximo
            ponto = ds.sel(latitude=lat_alvo, longitude=lon_alvo, method='nearest')

            # Extrai os valores
            u_val = ponto['uwnd'].values.item()
            v_val = ponto['vwnd'].values.item()

            resultados.append({
                'praia': info['nome'],
                'municipio': info['municipio'],
                'latitude': lat_alvo,
                'longitude': lon_alvo,
                'u_vento': u_val,
                'v_vento': v_val,
                'data': DATA_ALVO
            })

        ds.close()
        return pd.DataFrame(resultados)

    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")
        return None


# --- Execução ---
if baixar_dados(URL_COMPLETA, caminho_arquivo):
    df_resultados = extrair_dados_praias(caminho_arquivo, PRAIAS)
    if df_resultados is not None:
        print("\nDados de vento para as praias:")
        print(df_resultados)
        # Salva os resultados em CSV
        df_resultados.to_csv(f"vento_praias_{DATA_ALVO}.csv", index=False)
        print(f"Resultados salvos em 'vento_praias_{DATA_ALVO}.csv'")
    else:
        print("Falha ao extrair os dados.")
else:
    print("Falha no download. Verifique a data e a conexão.")