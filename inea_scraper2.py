# -*- coding: utf-8 -*-
"""
INEA Balneabilidade Scraper - Versão Completa com Geolocalização
Sistema de coleta automática de dados de balneabilidade das praias do Rio de Janeiro

Autor: Sistema Praias RJ
Versão: 2.0
"""

import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
import time
from dataclasses import dataclass, asdict, field

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('inea_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class Coordenadas:
    """Coordenadas geográficas"""
    latitude: float
    longitude: float
    precisao_metros: int = 50  # Raio de precisão do ponto central


@dataclass
class BalneabilidadeData:
    """Estrutura de dados completa para balneabilidade com geolocalização"""
    praia_id: str
    praia_nome: str
    status: str                        # 'propria' | 'impropria' | 'indisponivel'
    coliformes_fecais: Optional[int]
    data_coleta: str                   # formato: YYYY-MM-DD
    municipio: str
    regiao: str
    coordenadas: Optional[Dict]        # {'latitude': float, 'longitude': float}
    bairro: str = ""
    extensao_km: Optional[float] = None
    caracteristicas: List[str] = field(default_factory=list)
    fonte: str = "INEA"
    observacoes: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    url_inea: Optional[str] = None


class INEAScraper:
    """
    Scraper completo para dados de balneabilidade do INEA.
    Cobre todas as praias monitoradas no litoral do Rio de Janeiro,
    incluindo Baia de Guanabara, Costa Verde e Região dos Lagos.
    """

    BASE_URL = "http://www.inea.rj.gov.br"
    BALNEABILIDADE_URL = "http://200.20.53.17/"
    INEA_BALNEABILIDADE_URL = "https://www.inea.rj.gov.br/agua-solo-e-ar/balneabilidade/"

    # ------------------------------------------------------------------ #
    #  CADASTRO COMPLETO DE PRAIAS COM GEOLOCALIZAÇÃO                     #
    #  Fonte das coordenadas: centros aproximados de cada praia           #
    # ------------------------------------------------------------------ #
    PRAIAS = {
        # ── ZONA SUL ────────────────────────────────────────────────────
        "leme": {
            "nome": "Leme",
            "municipio": "Rio de Janeiro",
            "bairro": "Leme",
            "regiao": "Zona Sul",
            "coordenadas": {"latitude": -22.9635, "longitude": -43.1674},
            "extensao_km": 0.8,
            "caracteristicas": ["urbanizada", "familiar", "acessível"],
        },
        "copacabana": {
            "nome": "Copacabana",
            "municipio": "Rio de Janeiro",
            "bairro": "Copacabana",
            "regiao": "Zona Sul",
            "coordenadas": {"latitude": -22.9711, "longitude": -43.1823},
            "extensao_km": 4.0,
            "caracteristicas": ["urbanizada", "turística", "agitada"],
        },
        "arpoador": {
            "nome": "Arpoador",
            "municipio": "Rio de Janeiro",
            "bairro": "Ipanema",
            "regiao": "Zona Sul",
            "coordenadas": {"latitude": -22.9876, "longitude": -43.1940},
            "extensao_km": 0.5,
            "caracteristicas": ["surf", "pôr-do-sol", "rochosa"],
        },
        "ipanema": {
            "nome": "Ipanema",
            "municipio": "Rio de Janeiro",
            "bairro": "Ipanema",
            "regiao": "Zona Sul",
            "coordenadas": {"latitude": -22.9868, "longitude": -43.2040},
            "extensao_km": 2.5,
            "caracteristicas": ["urbanizada", "turística", "badalada"],
        },
        "leblon": {
            "nome": "Leblon",
            "municipio": "Rio de Janeiro",
            "bairro": "Leblon",
            "regiao": "Zona Sul",
            "coordenadas": {"latitude": -22.9877, "longitude": -43.2230},
            "extensao_km": 1.5,
            "caracteristicas": ["urbanizada", "nobre", "familiar"],
        },
        "vidigal": {
            "nome": "Vidigal",
            "municipio": "Rio de Janeiro",
            "bairro": "Vidigal",
            "regiao": "Zona Sul",
            "coordenadas": {"latitude": -22.9932, "longitude": -43.2349},
            "extensao_km": 0.4,
            "caracteristicas": ["pequena", "pitoresca"],
        },
        "sao_conrado": {
            "nome": "São Conrado",
            "municipio": "Rio de Janeiro",
            "bairro": "São Conrado",
            "regiao": "Zona Sul",
            "coordenadas": {"latitude": -23.0038, "longitude": -43.2753},
            "extensao_km": 1.7,
            "caracteristicas": ["voo-livre", "surf", "tranquila"],
        },
        "joatinga": {
            "nome": "Joatinga",
            "municipio": "Rio de Janeiro",
            "bairro": "Joá",
            "regiao": "Zona Sul",
            "coordenadas": {"latitude": -23.0102, "longitude": -43.2879},
            "extensao_km": 0.5,
            "caracteristicas": ["secreta", "acesso-limitado", "bela"],
        },
        "flamengo": {
            "nome": "Flamengo",
            "municipio": "Rio de Janeiro",
            "bairro": "Flamengo",
            "regiao": "Zona Sul",
            "coordenadas": {"latitude": -22.9295, "longitude": -43.1736},
            "extensao_km": 1.3,
            "caracteristicas": ["baía", "esporte", "parque"],
        },
        "botafogo": {
            "nome": "Botafogo",
            "municipio": "Rio de Janeiro",
            "bairro": "Botafogo",
            "regiao": "Zona Sul",
            "coordenadas": {"latitude": -22.9519, "longitude": -43.1820},
            "extensao_km": 0.9,
            "caracteristicas": ["baía", "histórica", "paisagem"],
        },
        "urca": {
            "nome": "Urca",
            "municipio": "Rio de Janeiro",
            "bairro": "Urca",
            "regiao": "Zona Sul",
            "coordenadas": {"latitude": -22.9486, "longitude": -43.1637},
            "extensao_km": 0.4,
            "caracteristicas": ["tranquila", "baía", "natureza"],
        },
        "vermelha": {
            "nome": "Vermelha",
            "municipio": "Rio de Janeiro",
            "bairro": "Urca",
            "regiao": "Zona Sul",
            "coordenadas": {"latitude": -22.9533, "longitude": -43.1607},
            "extensao_km": 0.3,
            "caracteristicas": ["pequena", "mergulho", "peixe-boi"],
        },
        "gloria": {
            "nome": "Glória",
            "municipio": "Rio de Janeiro",
            "bairro": "Glória",
            "regiao": "Zona Sul",
            "coordenadas": {"latitude": -22.9232, "longitude": -43.1740},
            "extensao_km": 0.5,
            "caracteristicas": ["baía", "histórica"],
        },

        # ── ZONA OESTE ──────────────────────────────────────────────────
        "barra": {
            "nome": "Barra da Tijuca",
            "municipio": "Rio de Janeiro",
            "bairro": "Barra da Tijuca",
            "regiao": "Zona Oeste",
            "coordenadas": {"latitude": -23.0048, "longitude": -43.3658},
            "extensao_km": 18.0,
            "caracteristicas": ["maior-praia", "surf", "modernа"],
        },
        "recreio": {
            "nome": "Recreio dos Bandeirantes",
            "municipio": "Rio de Janeiro",
            "bairro": "Recreio dos Bandeirantes",
            "regiao": "Zona Oeste",
            "coordenadas": {"latitude": -23.0241, "longitude": -43.4626},
            "extensao_km": 5.0,
            "caracteristicas": ["familiar", "tranquila", "natureza"],
        },
        "macumba": {
            "nome": "Macumba",
            "municipio": "Rio de Janeiro",
            "bairro": "Recreio dos Bandeirantes",
            "regiao": "Zona Oeste",
            "coordenadas": {"latitude": -23.0310, "longitude": -43.4921},
            "extensao_km": 1.5,
            "caracteristicas": ["surf", "alternativa", "jovem"],
        },
        "prainha": {
            "nome": "Prainha",
            "municipio": "Rio de Janeiro",
            "bairro": "Recreio dos Bandeirantes",
            "regiao": "Zona Oeste",
            "coordenadas": {"latitude": -23.0415, "longitude": -43.5043},
            "extensao_km": 0.7,
            "caracteristicas": ["surf", "preservada", "APA"],
        },
        "grumari": {
            "nome": "Grumari",
            "municipio": "Rio de Janeiro",
            "bairro": "Grumari",
            "regiao": "Zona Oeste",
            "coordenadas": {"latitude": -23.0548, "longitude": -43.5283},
            "extensao_km": 1.5,
            "caracteristicas": ["APA", "selvagem", "nudismo"],
        },
        "abricó": {
            "nome": "Abricó",
            "municipio": "Rio de Janeiro",
            "bairro": "Grumari",
            "regiao": "Zona Oeste",
            "coordenadas": {"latitude": -23.0582, "longitude": -43.5346},
            "extensao_km": 0.4,
            "caracteristicas": ["nudismo-oficial", "pequena", "isolada"],
        },
        "sepetiba": {
            "nome": "Sepetiba",
            "municipio": "Rio de Janeiro",
            "bairro": "Sepetiba",
            "regiao": "Zona Oeste",
            "coordenadas": {"latitude": -22.9774, "longitude": -43.7087},
            "extensao_km": 3.0,
            "caracteristicas": ["baía", "pesca", "tranquila"],
        },

        # ── REGIÃO METROPOLITANA / NITERÓI ──────────────────────────────
        "icarai": {
            "nome": "Icaraí",
            "municipio": "Niterói",
            "bairro": "Icaraí",
            "regiao": "Niterói",
            "coordenadas": {"latitude": -22.9035, "longitude": -43.1106},
            "extensao_km": 1.2,
            "caracteristicas": ["urbanizada", "familiar", "baía"],
        },
        "charitas": {
            "nome": "Charitas",
            "municipio": "Niterói",
            "bairro": "Charitas",
            "regiao": "Niterói",
            "coordenadas": {"latitude": -22.9231, "longitude": -43.1200},
            "extensao_km": 0.6,
            "caracteristicas": ["baía", "calma", "famílias"],
        },
        "jurujuba": {
            "nome": "Jurujuba",
            "municipio": "Niterói",
            "bairro": "Jurujuba",
            "regiao": "Niterói",
            "coordenadas": {"latitude": -22.9354, "longitude": -43.1118},
            "extensao_km": 0.5,
            "caracteristicas": ["pesca", "frutos-do-mar", "baía"],
        },
        "camboinhas": {
            "nome": "Camboinhas",
            "municipio": "Niterói",
            "bairro": "Camboinhas",
            "regiao": "Niterói",
            "coordenadas": {"latitude": -22.9645, "longitude": -43.0534},
            "extensao_km": 1.0,
            "caracteristicas": ["mar-aberto", "tranquila"],
        },
        "itacoatiara": {
            "nome": "Itacoatiara",
            "municipio": "Niterói",
            "bairro": "Itacoatiara",
            "regiao": "Niterói",
            "coordenadas": {"latitude": -22.9681, "longitude": -43.0356},
            "extensao_km": 1.5,
            "caracteristicas": ["surf", "pedras", "rochosa", "linda"],
        },
        "itaipu": {
            "nome": "Itaipu",
            "municipio": "Niterói",
            "bairro": "Itaipu",
            "regiao": "Niterói",
            "coordenadas": {"latitude": -22.9591, "longitude": -43.0493},
            "extensao_km": 2.0,
            "caracteristicas": ["pesca", "surf", "lagoa"],
        },
        "piratininga": {
            "nome": "Piratininga",
            "municipio": "Niterói",
            "bairro": "Piratininga",
            "regiao": "Niterói",
            "coordenadas": {"latitude": -22.9554, "longitude": -43.0588},
            "extensao_km": 1.8,
            "caracteristicas": ["lagoa", "surf", "kite"],
        },

        # ── REGIÃO DOS LAGOS ────────────────────────────────────────────
        "arraial_praia_grande": {
            "nome": "Praia Grande (Arraial do Cabo)",
            "municipio": "Arraial do Cabo",
            "bairro": "Centro",
            "regiao": "Região dos Lagos",
            "coordenadas": {"latitude": -22.9714, "longitude": -42.0173},
            "extensao_km": 2.5,
            "caracteristicas": ["cristalina", "mergulho", "turismo"],
        },
        "arraial_pontal": {
            "nome": "Pontal do Atalaia (Arraial do Cabo)",
            "municipio": "Arraial do Cabo",
            "bairro": "Pontal",
            "regiao": "Região dos Lagos",
            "coordenadas": {"latitude": -22.9649, "longitude": -42.0297},
            "extensao_km": 1.0,
            "caracteristicas": ["mergulho", "snorkel", "barco"],
        },
        "cabo_frio_forte": {
            "nome": "Praia do Forte (Cabo Frio)",
            "municipio": "Cabo Frio",
            "bairro": "Centro",
            "regiao": "Região dos Lagos",
            "coordenadas": {"latitude": -22.8776, "longitude": -42.0190},
            "extensao_km": 1.5,
            "caracteristicas": ["dunas", "kite", "ventos"],
        },
        "cabo_frio_peró": {
            "nome": "Peró (Cabo Frio)",
            "municipio": "Cabo Frio",
            "bairro": "Peró",
            "regiao": "Região dos Lagos",
            "coordenadas": {"latitude": -22.8445, "longitude": -42.0608},
            "extensao_km": 7.0,
            "caracteristicas": ["APA", "dunas", "selvagem"],
        },
        "buzios_geribá": {
            "nome": "Geribá (Búzios)",
            "municipio": "Armação dos Búzios",
            "bairro": "Geribá",
            "regiao": "Região dos Lagos",
            "coordenadas": {"latitude": -22.7740, "longitude": -41.8926},
            "extensao_km": 2.5,
            "caracteristicas": ["surf", "festas", "jovem"],
        },
        "buzios_ferradura": {
            "nome": "Ferradura (Búzios)",
            "municipio": "Armação dos Búzios",
            "bairro": "Ferradura",
            "regiao": "Região dos Lagos",
            "coordenadas": {"latitude": -22.7838, "longitude": -41.9001},
            "extensao_km": 0.8,
            "caracteristicas": ["calma", "familiar", "catamarã"],
        },
        "buzios_joao_fernandes": {
            "nome": "João Fernandes (Búzios)",
            "municipio": "Armação dos Búzios",
            "bairro": "João Fernandes",
            "regiao": "Região dos Lagos",
            "coordenadas": {"latitude": -22.7485, "longitude": -41.8856},
            "extensao_km": 0.6,
            "caracteristicas": ["mergulho", "snorkel", "clear"],
        },
        "saquarema": {
            "nome": "Saquarema",
            "municipio": "Saquarema",
            "bairro": "Itaúna",
            "regiao": "Região dos Lagos",
            "coordenadas": {"latitude": -22.9249, "longitude": -42.5104},
            "extensao_km": 4.0,
            "caracteristicas": ["surf", "campeonatos", "vento"],
        },
        "araruama": {
            "nome": "Araruama",
            "municipio": "Araruama",
            "bairro": "Centro",
            "regiao": "Região dos Lagos",
            "coordenadas": {"latitude": -22.8726, "longitude": -42.3421},
            "extensao_km": 3.0,
            "caracteristicas": ["lagoa", "kite", "vento"],
        },

        # ── COSTA VERDE ─────────────────────────────────────────────────
        "angra_bonfim": {
            "nome": "Bonfim (Angra dos Reis)",
            "municipio": "Angra dos Reis",
            "bairro": "Bonfim",
            "regiao": "Costa Verde",
            "coordenadas": {"latitude": -23.0014, "longitude": -44.3139},
            "extensao_km": 1.0,
            "caracteristicas": ["baía", "camping", "natureza"],
        },
        "angra_do_col": {
            "nome": "Do Col (Angra dos Reis)",
            "municipio": "Angra dos Reis",
            "bairro": "Centro",
            "regiao": "Costa Verde",
            "coordenadas": {"latitude": -22.9678, "longitude": -44.3082},
            "extensao_km": 0.6,
            "caracteristicas": ["baía", "mergulho", "ilha-perto"],
        },
        "paraty_paraty_mirim": {
            "nome": "Paraty-Mirim",
            "municipio": "Paraty",
            "bairro": "Paraty-Mirim",
            "regiao": "Costa Verde",
            "coordenadas": {"latitude": -23.2670, "longitude": -44.6250},
            "extensao_km": 1.2,
            "caracteristicas": ["histórica", "preservada", "turismo"],
        },
        "paraty_trindade": {
            "nome": "Trindade (Paraty)",
            "municipio": "Paraty",
            "bairro": "Trindade",
            "regiao": "Costa Verde",
            "coordenadas": {"latitude": -23.3296, "longitude": -44.7218},
            "extensao_km": 1.0,
            "caracteristicas": ["hippie", "lagoa", "cachoeira"],
        },
        "mangaratiba": {
            "nome": "Mangaratiba",
            "municipio": "Mangaratiba",
            "bairro": "Centro",
            "regiao": "Costa Verde",
            "coordenadas": {"latitude": -22.9591, "longitude": -44.0418},
            "extensao_km": 1.5,
            "caracteristicas": ["pesca", "baía", "embarque-ilhas"],
        },

        # ── BAIA DE GUANABARA ───────────────────────────────────────────
        "ramos": {
            "nome": "Ramos",
            "municipio": "Rio de Janeiro",
            "bairro": "Ramos",
            "regiao": "Baía de Guanabara",
            "coordenadas": {"latitude": -22.8608, "longitude": -43.2446},
            "extensao_km": 1.0,
            "caracteristicas": ["baía", "periférica"],
        },
        "olaria": {
            "nome": "Olaria",
            "municipio": "Rio de Janeiro",
            "bairro": "Olaria",
            "regiao": "Baía de Guanabara",
            "coordenadas": {"latitude": -22.8524, "longitude": -43.2658},
            "extensao_km": 0.8,
            "caracteristicas": ["baía", "periférica"],
        },
        "ilha_do_governador": {
            "nome": "Praia da Bica (Ilha do Governador)",
            "municipio": "Rio de Janeiro",
            "bairro": "Ilha do Governador",
            "regiao": "Baía de Guanabara",
            "coordenadas": {"latitude": -22.8012, "longitude": -43.1952},
            "extensao_km": 0.5,
            "caracteristicas": ["ilha", "baía", "pesca"],
        },
        "paqueta": {
            "nome": "Paquetá",
            "municipio": "Rio de Janeiro",
            "bairro": "Ilha de Paquetá",
            "regiao": "Baía de Guanabara",
            "coordenadas": {"latitude": -22.7681, "longitude": -43.1082},
            "extensao_km": 0.8,
            "caracteristicas": ["ilha", "bicicleta", "sem-carros", "histórica"],
        },
    }

    def __init__(self, timeout: int = 30, retry_attempts: int = 3):
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml',
            'Accept-Language': 'pt-BR,pt;q=0.9',
        })

    # ------------------------------------------------------------------ #
    #  REQUISIÇÃO HTTP                                                     #
    # ------------------------------------------------------------------ #

    def _fazer_requisicao(self, url: str) -> Optional[requests.Response]:
        """Faz requisição HTTP com retry automático e backoff exponencial"""
        for tentativa in range(self.retry_attempts):
            try:
                logger.info(f"Requisição: {url} (tentativa {tentativa + 1}/{self.retry_attempts})")
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout na tentativa {tentativa + 1}")
            except requests.exceptions.ConnectionError:
                logger.warning(f"Erro de conexão na tentativa {tentativa + 1}")
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP {e.response.status_code} na tentativa {tentativa + 1}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Erro: {str(e)}")

            if tentativa < self.retry_attempts - 1:
                espera = 2 ** tentativa
                logger.info(f"Aguardando {espera}s antes de nova tentativa...")
                time.sleep(espera)

        logger.error(f"Falha após {self.retry_attempts} tentativas: {url}")
        return None

    # ------------------------------------------------------------------ #
    #  DETERMINAÇÃO DE STATUS                                              #
    # ------------------------------------------------------------------ #

    def _determinar_status(self, texto_status: str, coliformes: Optional[int] = None) -> str:
        """Determina status da praia baseado no texto ou valor de coliformes"""
        texto = texto_status.lower().strip()

        if any(p in texto for p in ['própria', 'propria', 'adequada', 'boa']):
            return 'propria'
        if any(p in texto for p in ['imprópria', 'impropria', 'inadequada', 'ruim', 'péssima']):
            return 'impropria'

        # Padrão CONAMA 274/2000: limite de 800 NMP/100ml para banho
        if coliformes is not None:
            return 'propria' if coliformes < 800 else 'impropria'

        return 'indisponivel'

    def _extrair_coliformes(self, texto: str) -> Optional[int]:
        """Extrai valor numérico de coliformes fecais do texto"""
        padrao = re.search(r'(\d[\d\.,]*)\s*(NMP|UFC|col)?', texto, re.IGNORECASE)
        if padrao:
            try:
                valor = padrao.group(1).replace('.', '').replace(',', '')
                return int(valor)
            except ValueError:
                pass
        return None

    # ------------------------------------------------------------------ #
    #  SCRAPING PRINCIPAL                                                  #
    # ------------------------------------------------------------------ #

    def scrape_balneabilidade(self) -> List[BalneabilidadeData]:
        """
        Coleta dados de balneabilidade. Tenta múltiplas URLs do INEA.
        Em caso de falha, retorna dados simulados para desenvolvimento.
        """
        logger.info("=" * 60)
        logger.info("Iniciando coleta de dados de balneabilidade")
        logger.info("=" * 60)

        urls_tentativas = [
            self.BALNEABILIDADE_URL,
            self.INEA_BALNEABILIDADE_URL,
            f"{self.BASE_URL}/balneabilidade/",
        ]

        for url in urls_tentativas:
            response = self._fazer_requisicao(url)
            if response:
                resultados = self._parse_html(response)
                if resultados:
                    # Enriquecer com dados geográficos
                    resultados = self._enriquecer_com_geodados(resultados)
                    logger.info(f"✅ Coletadas {len(resultados)} praias de {url}")
                    return resultados

        logger.warning("⚠️  Scraping real falhou. Usando dados simulados.")
        return self._gerar_dados_mockados()

    def _parse_html(self, response: requests.Response) -> List[BalneabilidadeData]:
        """
        Parse do HTML retornado pelo INEA.
        Tenta múltiplas estratégias de extração.
        """
        soup = BeautifulSoup(response.content, 'html.parser')
        resultados = []

        # Estratégia 1: tabelas
        tabelas = soup.find_all('table')
        for tabela in tabelas:
            linhas = tabela.find_all('tr')
            for linha in linhas[1:]:
                colunas = linha.find_all(['td', 'th'])
                if len(colunas) < 2:
                    continue
                try:
                    nome = colunas[0].get_text(strip=True)
                    status_texto = colunas[1].get_text(strip=True)
                    coliformes_texto = colunas[2].get_text(strip=True) if len(colunas) > 2 else ""
                    data_texto = colunas[3].get_text(strip=True) if len(colunas) > 3 else ""

                    praia_id, praia_info = self._identificar_praia(nome)
                    if not praia_info:
                        continue

                    coliformes = self._extrair_coliformes(coliformes_texto)
                    data_coleta = self._extrair_data(data_texto) or datetime.now().strftime('%Y-%m-%d')

                    resultado = BalneabilidadeData(
                        praia_id=praia_id,
                        praia_nome=praia_info['nome'],
                        status=self._determinar_status(status_texto, coliformes),
                        coliformes_fecais=coliformes,
                        data_coleta=data_coleta,
                        municipio=praia_info['municipio'],
                        bairro=praia_info.get('bairro', ''),
                        regiao=praia_info['regiao'],
                        coordenadas=praia_info.get('coordenadas'),
                        extensao_km=praia_info.get('extensao_km'),
                        caracteristicas=praia_info.get('caracteristicas', []),
                        fonte="INEA",
                        url_inea=response.url,
                    )
                    resultados.append(resultado)
                except Exception as e:
                    logger.debug(f"Erro ao processar linha: {e}")
                    continue

        # Estratégia 2: divs/spans com classes específicas
        if not resultados:
            resultados = self._parse_html_estrutura_alternativa(soup)

        return resultados

    def _parse_html_estrutura_alternativa(self, soup: BeautifulSoup) -> List[BalneabilidadeData]:
        """Parsing alternativo para estruturas HTML diferentes de tabela"""
        resultados = []
        # Busca por padrões comuns em portais governamentais
        for elem in soup.find_all(['div', 'li', 'article'], class_=re.compile(r'praia|beach|balneab', re.I)):
            texto = elem.get_text(separator=' ', strip=True)
            praia_id, praia_info = self._identificar_praia(texto)
            if praia_info:
                status = self._determinar_status(texto)
                coliformes = self._extrair_coliformes(texto)
                resultado = BalneabilidadeData(
                    praia_id=praia_id,
                    praia_nome=praia_info['nome'],
                    status=status,
                    coliformes_fecais=coliformes,
                    data_coleta=datetime.now().strftime('%Y-%m-%d'),
                    municipio=praia_info['municipio'],
                    bairro=praia_info.get('bairro', ''),
                    regiao=praia_info['regiao'],
                    coordenadas=praia_info.get('coordenadas'),
                    extensao_km=praia_info.get('extensao_km'),
                    caracteristicas=praia_info.get('caracteristicas', []),
                )
                resultados.append(resultado)
        return resultados

    def _identificar_praia(self, texto: str) -> Tuple[Optional[str], Optional[Dict]]:
        """Tenta identificar uma praia do cadastro a partir de texto livre"""
        texto_lower = texto.lower()
        melhor_match = None
        melhor_id = None
        max_score = 0

        for pid, info in self.PRAIAS.items():
            score = 0
            nome_lower = info['nome'].lower()

            if nome_lower in texto_lower:
                score += 10
            elif pid.replace('_', ' ') in texto_lower:
                score += 8
            else:
                # Busca por partes do nome
                partes = nome_lower.split()
                for parte in partes:
                    if len(parte) > 3 and parte in texto_lower:
                        score += 3

            if score > max_score:
                max_score = score
                melhor_match = info
                melhor_id = pid

        if max_score >= 8:
            return melhor_id, melhor_match
        return None, None

    def _extrair_data(self, texto: str) -> Optional[str]:
        """Extrai data do texto em vários formatos"""
        padroes = [
            r'(\d{4})-(\d{2})-(\d{2})',           # YYYY-MM-DD
            r'(\d{2})/(\d{2})/(\d{4})',             # DD/MM/YYYY
            r'(\d{2})-(\d{2})-(\d{4})',             # DD-MM-YYYY
        ]
        for padrao in padroes:
            m = re.search(padrao, texto)
            if m:
                try:
                    grupos = m.groups()
                    if len(grupos[0]) == 4:
                        return f"{grupos[0]}-{grupos[1]}-{grupos[2]}"
                    else:
                        return f"{grupos[2]}-{grupos[1]}-{grupos[0]}"
                except Exception:
                    pass
        return None

    def _enriquecer_com_geodados(self, dados: List[BalneabilidadeData]) -> List[BalneabilidadeData]:
        """Preenche coordenadas e metadados geográficos faltantes via cadastro local"""
        for dado in dados:
            if dado.praia_id in self.PRAIAS:
                info = self.PRAIAS[dado.praia_id]
                if not dado.coordenadas:
                    dado.coordenadas = info.get('coordenadas')
                if not dado.bairro:
                    dado.bairro = info.get('bairro', '')
                if not dado.extensao_km:
                    dado.extensao_km = info.get('extensao_km')
                if not dado.caracteristicas:
                    dado.caracteristicas = info.get('caracteristicas', [])
        return dados

    # ------------------------------------------------------------------ #
    #  DADOS SIMULADOS (DESENVOLVIMENTO)                                   #
    # ------------------------------------------------------------------ #

    def _gerar_dados_mockados(self) -> List[BalneabilidadeData]:
        """
        Gera dados simulados realistas para desenvolvimento.
        ATENÇÃO: Remover ou desabilitar em produção.
        """
        import random
        random.seed(42)  # Seed fixo para resultados reproduzíveis
        resultados = []

        # Probabilidades de contaminação por região (baseadas em dados históricos)
        prob_impropria = {
            "Zona Sul": 0.15,
            "Zona Oeste": 0.10,
            "Niterói": 0.20,
            "Região dos Lagos": 0.12,
            "Costa Verde": 0.08,
            "Baía de Guanabara": 0.65,  # Baía historicamente mais contaminada
        }

        for praia_id, info in self.PRAIAS.items():
            regiao = info['regiao']
            p_imp = prob_impropria.get(regiao, 0.2)
            status = 'impropria' if random.random() < p_imp else 'propria'

            if status == 'propria':
                coliformes = random.randint(50, 799)
            else:
                coliformes = random.randint(800, 2500)

            resultado = BalneabilidadeData(
                praia_id=praia_id,
                praia_nome=info['nome'],
                status=status,
                coliformes_fecais=coliformes,
                data_coleta=datetime.now().strftime('%Y-%m-%d'),
                municipio=info['municipio'],
                bairro=info.get('bairro', ''),
                regiao=regiao,
                coordenadas=info.get('coordenadas'),
                extensao_km=info.get('extensao_km'),
                caracteristicas=info.get('caracteristicas', []),
                fonte="MOCK_DATA",
                observacoes="⚠️ Dados simulados — não usar para decisões reais",
            )
            resultados.append(resultado)

        return resultados

    # ------------------------------------------------------------------ #
    #  FILTROS E CONSULTAS                                                 #
    # ------------------------------------------------------------------ #

    def filtrar_por_regiao(self, dados: List[BalneabilidadeData], regiao: str) -> List[BalneabilidadeData]:
        """Filtra praias por região"""
        return [d for d in dados if d.regiao.lower() == regiao.lower()]

    def filtrar_por_municipio(self, dados: List[BalneabilidadeData], municipio: str) -> List[BalneabilidadeData]:
        """Filtra praias por município"""
        return [d for d in dados if d.municipio.lower() == municipio.lower()]

    def filtrar_proprias(self, dados: List[BalneabilidadeData]) -> List[BalneabilidadeData]:
        """Retorna apenas praias próprias para banho"""
        return [d for d in dados if d.status == 'propria']

    def praias_proximas(
        self,
        dados: List[BalneabilidadeData],
        latitude: float,
        longitude: float,
        raio_km: float = 10.0,
    ) -> List[BalneabilidadeData]:
        """
        Retorna praias dentro de um raio (km) de uma coordenada.
        Usa fórmula de Haversine para distância geodésica.
        """
        from math import radians, cos, sin, asin, sqrt

        def haversine(lat1, lon1, lat2, lon2):
            R = 6371  # raio da Terra em km
            dlat = radians(lat2 - lat1)
            dlon = radians(lon2 - lon1)
            a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
            return 2 * R * asin(sqrt(a))

        resultado = []
        for dado in dados:
            if dado.coordenadas:
                dist = haversine(
                    latitude, longitude,
                    dado.coordenadas['latitude'],
                    dado.coordenadas['longitude'],
                )
                if dist <= raio_km:
                    resultado.append((dist, dado))

        resultado.sort(key=lambda x: x[0])
        return [d for _, d in resultado]

    # ------------------------------------------------------------------ #
    #  EXPORTAÇÃO                                                          #
    # ------------------------------------------------------------------ #

    def exportar_json(self, dados: List[BalneabilidadeData], arquivo: str = 'balneabilidade.json'):
        """Exporta todos os dados para JSON"""
        dados_dict = [asdict(d) for d in dados]
        with open(arquivo, 'w', encoding='utf-8') as f:
            json.dump(dados_dict, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ JSON exportado: {arquivo} ({len(dados_dict)} registros)")

    def exportar_geojson(self, dados: List[BalneabilidadeData], arquivo: str = 'balneabilidade.geojson'):
        """
        Exporta em formato GeoJSON — compatível com Leaflet, QGIS,
        Google Maps, Mapbox e qualquer cliente de mapas.
        """
        features = []
        for d in dados:
            if not d.coordenadas:
                continue

            cor = {
                'propria': '#2ecc71',
                'impropria': '#e74c3c',
                'indisponivel': '#95a5a6',
            }.get(d.status, '#95a5a6')

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [d.coordenadas['longitude'], d.coordenadas['latitude']],
                },
                "properties": {
                    "id": d.praia_id,
                    "nome": d.praia_nome,
                    "status": d.status,
                    "municipio": d.municipio,
                    "bairro": d.bairro,
                    "regiao": d.regiao,
                    "coliformes_fecais": d.coliformes_fecais,
                    "extensao_km": d.extensao_km,
                    "caracteristicas": d.caracteristicas,
                    "data_coleta": d.data_coleta,
                    "fonte": d.fonte,
                    "observacoes": d.observacoes,
                    "marker-color": cor,
                    "marker-symbol": "beach" if d.status == 'propria' else "no-entry",
                },
            }
            features.append(feature)

        geojson = {
            "type": "FeatureCollection",
            "metadata": {
                "gerado_em": datetime.now().isoformat(),
                "total_praias": len(features),
                "sistema": "INEA Balneabilidade Scraper v2.0",
            },
            "features": features,
        }

        with open(arquivo, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ GeoJSON exportado: {arquivo} ({len(features)} pontos)")

    def exportar_csv(self, dados: List[BalneabilidadeData], arquivo: str = 'balneabilidade.csv'):
        """Exporta para CSV com colunas de lat/lon separadas"""
        import csv
        with open(arquivo, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'praia_id', 'nome', 'status', 'municipio', 'bairro', 'regiao',
                'latitude', 'longitude', 'coliformes_fecais', 'extensao_km',
                'data_coleta', 'fonte', 'observacoes',
            ])
            for d in dados:
                lat = d.coordenadas['latitude'] if d.coordenadas else ''
                lon = d.coordenadas['longitude'] if d.coordenadas else ''
                writer.writerow([
                    d.praia_id, d.praia_nome, d.status, d.municipio, d.bairro,
                    d.regiao, lat, lon, d.coliformes_fecais, d.extensao_km,
                    d.data_coleta, d.fonte, d.observacoes,
                ])
        logger.info(f"✅ CSV exportado: {arquivo}")

    # ------------------------------------------------------------------ #
    #  ESTATÍSTICAS                                                        #
    # ------------------------------------------------------------------ #

    def obter_estatisticas(self, dados: List[BalneabilidadeData]) -> Dict:
        """Calcula estatísticas gerais e por região/município"""
        total = len(dados)
        proprias = sum(1 for d in dados if d.status == 'propria')
        improprias = sum(1 for d in dados if d.status == 'impropria')
        indisponiveis = sum(1 for d in dados if d.status == 'indisponivel')

        por_regiao: Dict[str, Dict] = {}
        for d in dados:
            r = d.regiao
            if r not in por_regiao:
                por_regiao[r] = {'total': 0, 'proprias': 0, 'improprias': 0}
            por_regiao[r]['total'] += 1
            if d.status == 'propria':
                por_regiao[r]['proprias'] += 1
            elif d.status == 'impropria':
                por_regiao[r]['improprias'] += 1

        coliformes_vals = [d.coliformes_fecais for d in dados if d.coliformes_fecais is not None]
        media_coliformes = round(sum(coliformes_vals) / len(coliformes_vals), 1) if coliformes_vals else None

        return {
            'total_praias': total,
            'praias_proprias': proprias,
            'praias_improprias': improprias,
            'praias_indisponiveis': indisponiveis,
            'percentual_proprias': round((proprias / total * 100) if total > 0 else 0, 1),
            'media_coliformes': media_coliformes,
            'por_regiao': por_regiao,
            'municipios': list({d.municipio for d in dados}),
            'data_referencia': datetime.now().strftime('%Y-%m-%d %H:%M'),
        }


# ------------------------------------------------------------------ #
#  MAIN                                                                #
# ------------------------------------------------------------------ #

def main():
    print("=" * 60)
    print("  INEA BALNEABILIDADE SCRAPER v2.0")
    print("  Cobertura: Rio de Janeiro completo + Niterói +")
    print("  Região dos Lagos + Costa Verde")
    print("=" * 60)

    scraper = INEAScraper()
    dados = scraper.scrape_balneabilidade()

    # Exibir resultados agrupados por região
    regioes = {}
    for d in dados:
        regioes.setdefault(d.regiao, []).append(d)

    for regiao, praias in sorted(regioes.items()):
        print(f"\n📍 {regiao.upper()}")
        print("-" * 55)
        for d in praias:
            emoji = "🟢" if d.status == 'propria' else ("🔴" if d.status == 'impropria' else "⚫")
            col_str = f"{d.coliformes_fecais} NMP" if d.coliformes_fecais else "N/D"
            coord_str = f"({d.coordenadas['latitude']:.4f}, {d.coordenadas['longitude']:.4f})" if d.coordenadas else ""
            print(f"  {emoji} {d.praia_nome:<30} {d.status.upper():<12} {col_str:<12} {coord_str}")

    # Estatísticas
    stats = scraper.obter_estatisticas(dados)
    print(f"\n{'=' * 60}")
    print(f"📊 RESUMO GERAL")
    print(f"{'=' * 60}")
    print(f"  Total de praias monitoradas : {stats['total_praias']}")
    print(f"  Próprias para banho         : {stats['praias_proprias']} ({stats['percentual_proprias']}%)")
    print(f"  Impróprias para banho       : {stats['praias_improprias']}")
    print(f"  Sem dados disponíveis       : {stats['praias_indisponiveis']}")
    if stats['media_coliformes']:
        print(f"  Média de coliformes fecais  : {stats['media_coliformes']} NMP/100ml")
    print(f"\n  Por Região:")
    for reg, info in sorted(stats['por_regiao'].items()):
        pct = round(info['proprias'] / info['total'] * 100, 1) if info['total'] else 0
        print(f"    {reg:<28} {info['proprias']}/{info['total']} próprias ({pct}%)")

    # Exportar
    scraper.exportar_json(dados, 'balneabilidade.json')
    scraper.exportar_geojson(dados, 'balneabilidade.geojson')
    scraper.exportar_csv(dados, 'balneabilidade.csv')

    # Exemplo: praias próximas ao Centro do Rio (lat=-22.906, lon=-43.172) em 15km
    print(f"\n{'=' * 60}")
    print("📍 PRAIAS PRÓPRIAS ATÉ 15KM DO CENTRO DO RIO")
    print(f"{'=' * 60}")
    proximas = scraper.praias_proximas(dados, -22.906, -43.172, raio_km=15)
    proprias_proximas = [p for p in proximas if p.status == 'propria']
    for p in proprias_proximas:
        print(f"  🟢 {p.praia_nome} ({p.municipio})")

    print(f"\n✅ Arquivos gerados: balneabilidade.json | .geojson | .csv")


if __name__ == "__main__":
    main()