# -*- coding: utf-8 -*-
"""
INEA Balneabilidade Scraper - v3.0
Estratégia de coleta em duas camadas:
  1. praialimpa.net  → rápido, HTML simples, dados semanais
  2. PDF do INEA     → fallback quando praialimpa.net tiver dados > 7 dias

Autor: Sistema Praias RJ
"""

import re
import json
import logging
import time
import io
from datetime import datetime, date, timedelta
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import pdfplumber

# ------------------------------------------------------------------ #
#  LOGGING                                                             #
# ------------------------------------------------------------------ #

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('inea_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  DATACLASSES                                                         #
# ------------------------------------------------------------------ #

@dataclass
class Coordenadas:
    latitude: float
    longitude: float
    precisao_metros: int = 50


@dataclass
class BalneabilidadeData:
    praia_id: str
    praia_nome: str
    status: str                        # 'propria' | 'impropria' | 'indisponivel'
    coliformes_fecais: Optional[int]
    data_coleta: str                   # YYYY-MM-DD
    municipio: str
    regiao: str
    coordenadas: Optional[Dict]
    bairro: str = ""
    extensao_km: Optional[float] = None
    caracteristicas: List[str] = field(default_factory=list)
    fonte: str = "INEA"
    observacoes: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    url_inea: Optional[str] = None


# ------------------------------------------------------------------ #
#  SCRAPER PRINCIPAL                                                   #
# ------------------------------------------------------------------ #

class INEAScraper:
    """
    Coleta dados de balneabilidade em duas camadas:
      1. praialimpa.net  (HTML simples, atualizado semanalmente)
      2. PDF do INEA     (fallback quando praialimpa.net > 7 dias)
    """

    PRAIALIMPA_URL       = "https://praialimpa.net/"
    INEA_BALNEABILIDADE_URL = "https://www.inea.rj.gov.br/ar-agua-e-solo/balneabilidade-das-praias/"
    MAX_IDADE_DIAS       = 14   # praialimpa.net considerado "fresco" até 7 dias

    # ── Cadastro de praias com coordenadas ─────────────────────────────
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
    }

    def __init__(self, timeout: int = 10, retry_attempts: int = 2):
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9',
        })

    # ------------------------------------------------------------------ #
    #  HTTP                                                                #
    # ------------------------------------------------------------------ #

    def _get(self, url: str) -> Optional[requests.Response]:
        """GET com retry e backoff exponencial."""
        for tentativa in range(self.retry_attempts):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                return resp
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout ({tentativa+1}/{self.retry_attempts}): {url}")
            except requests.exceptions.ConnectionError:
                logger.warning(f"Conexão falhou ({tentativa+1}/{self.retry_attempts}): {url}")
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP {e.response.status_code} ({tentativa+1}/{self.retry_attempts}): {url}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Erro ({tentativa+1}/{self.retry_attempts}): {e}")

            if tentativa < self.retry_attempts - 1:
                time.sleep(2 ** tentativa)

        logger.error(f"Falha após {self.retry_attempts} tentativas: {url}")
        return None

    # ------------------------------------------------------------------ #
    #  PONTO DE ENTRADA PRINCIPAL                                          #
    # ------------------------------------------------------------------ #

    def scrape_balneabilidade(self) -> List[BalneabilidadeData]:
        """
        Coleta dados com estratégia em duas camadas:
          1. praialimpa.net  — se dados ≤ 7 dias → usa direto
          2. PDF INEA        — se dados > 7 dias ou falha
        Nunca retorna dados mockados silenciosamente.
        """
        logger.info("=" * 60)
        logger.info("Iniciando coleta de balneabilidade (v3.0)")
        logger.info("=" * 60)

        # ── Camada 1: praialimpa.net ──────────────────────────────────
        logger.info("[ Camada 1 ] Tentando praialimpa.net...")
        dados_praialimpa, data_praialimpa = self._scrape_praialimpa()

        if dados_praialimpa:
            idade = (date.today() - data_praialimpa).days
            logger.info(f"praialimpa.net: {len(dados_praialimpa)} praias, data={data_praialimpa} ({idade} dias atrás)")

            if idade <= self.MAX_IDADE_DIAS:
                logger.info(f"✅ Dados frescos (≤{self.MAX_IDADE_DIAS} dias). Usando praialimpa.net.")
                return dados_praialimpa
            else:
                logger.warning(f"⚠️  Dados desatualizados ({idade} dias). Buscando PDF do INEA...")
        else:
            logger.warning("⚠️  praialimpa.net não retornou dados. Buscando PDF do INEA...")

        # ── Camada 2: PDF do INEA ─────────────────────────────────────
        logger.info("[ Camada 2 ] Tentando PDF do INEA...")
        dados_pdf = self._scrape_pdf_inea()

        if dados_pdf:
            logger.info(f"✅ PDF do INEA: {len(dados_pdf)} praias coletadas.")
            return dados_pdf

        # ── Sem dados reais disponíveis ───────────────────────────────
        raise RuntimeError(
            "Não foi possível coletar dados reais de balneabilidade.\n"
            "  - praialimpa.net: falhou ou dados > 7 dias\n"
            "  - PDF INEA: falhou\n"
            "Verifique a conectividade e os logs em inea_scraper.log"
        )

    # ================================================================== #
    #  CAMADA 1 — praialimpa.net                                          #
    # ================================================================== #

    def _scrape_praialimpa(self) -> Tuple[List[BalneabilidadeData], Optional[date]]:
        """
        Faz scraping do praialimpa.net.
        Retorna (lista_de_dados, data_de_atualizacao).
        Em caso de falha retorna ([], None).
        """
        resp = self._get(self.PRAIALIMPA_URL)
        if not resp:
            return [], None

        soup = BeautifulSoup(resp.content, 'html.parser')
        resultados: List[BalneabilidadeData] = []
        data_atualizacao: Optional[date] = None

        # A página lista município como <h1>, depois pares de <p> status + nome
        # Detectamos a data via texto "Atualizado em DD/MM/AAAA"
        texto_completo = soup.get_text(separator='\n')
        data_atualizacao = self._extrair_data_mais_recente(texto_completo)

        # Itera por município (seções <h1> + blocos)
        # Estrutura real: cada praia aparece como dois <p> consecutivos:
        #   <p class="proprio|improprio">Própria / Imprópria</p>
        #   <p><strong>Nome da Praia</strong> Trecho...</p>
        # Vamos extrair diretamente por texto de cada elemento

        municipio_atual = "Rio de Janeiro"
        for elem in soup.find_all(['h1', 'p']):
            tag = elem.name
            texto = elem.get_text(strip=True)

            if tag == 'h1' and texto and texto not in ('PraiaLimpa.net',):
                municipio_atual = texto
                continue

            # Detecta bloco de praia: começa com "Própria" ou "Imprópria"
            if texto in ('Própria', 'Imprópria', 'n/a'):
                status_texto = texto
                # Próximo <p> é o nome + trecho
                proximo = elem.find_next_sibling('p')
                if not proximo:
                    continue
                texto_prox = proximo.get_text(separator=' ', strip=True)

                # Nome é o conteúdo do <strong>
                strong = proximo.find('strong')
                if strong:
                    nome_praia = strong.get_text(strip=True)
                    trecho = texto_prox.replace(nome_praia, '').strip()
                else:
                    nome_praia = texto_prox
                    trecho = ''

                if not nome_praia:
                    continue

                status = self._determinar_status(status_texto)
                praia_id, praia_info = self._identificar_praia(nome_praia)

                data_str = data_atualizacao.strftime('%Y-%m-%d') if data_atualizacao else datetime.now().strftime('%Y-%m-%d')

                resultado = BalneabilidadeData(
                    praia_id=praia_id or self._slugify(nome_praia),
                    praia_nome=nome_praia,
                    status=status,
                    coliformes_fecais=None,
                    data_coleta=data_str,
                    municipio=municipio_atual,
                    regiao=praia_info['regiao'] if praia_info else self._inferir_regiao(municipio_atual),
                    coordenadas=praia_info.get('coordenadas') if praia_info else None,
                    bairro=praia_info.get('bairro', '') if praia_info else '',
                    extensao_km=praia_info.get('extensao_km') if praia_info else None,
                    caracteristicas=praia_info.get('caracteristicas', []) if praia_info else [],
                    fonte="praialimpa.net",
                    observacoes=trecho if trecho else None,
                    url_inea=self.PRAIALIMPA_URL,
                )
                resultados.append(resultado)

        # Remove duplicatas (mesma praia, múltiplos pontos) — mantém o mais recente
        resultados = self._consolidar_por_praia(resultados)

        return resultados, data_atualizacao

    def _extrair_data_mais_recente(self, texto: str) -> Optional[date]:
        """Extrai a data mais recente do padrão 'Atualizado em DD/MM/AAAA'."""
        matches = re.findall(r'Atualizado em (\d{2}/\d{2}/\d{4})', texto)
        datas = []
        for m in matches:
            try:
                datas.append(datetime.strptime(m, '%d/%m/%Y').date())
            except ValueError:
                pass
        return max(datas) if datas else None

    def _consolidar_por_praia(self, dados: List[BalneabilidadeData]) -> List[BalneabilidadeData]:
        """
        praialimpa.net lista múltiplos pontos por praia (ex: Copacabana tem 4 pontos).
        Consolida em uma entrada por praia: se QUALQUER ponto for impróprio → imprópria.
        Mantém observações dos trechos impróprios.
        """
        por_praia: Dict[str, BalneabilidadeData] = {}
        obs_improprias: Dict[str, List[str]] = {}

        for d in dados:
            chave = d.praia_nome.lower()
            if chave not in por_praia:
                por_praia[chave] = d
                obs_improprias[chave] = []
            else:
                # Já existe: atualiza status se houver ponto impróprio
                if d.status == 'impropria':
                    por_praia[chave].status = 'impropria'
                    if d.observacoes:
                        obs_improprias[chave].append(d.observacoes)

        # Injeta observações dos trechos problemáticos
        for chave, trechos in obs_improprias.items():
            if trechos:
                por_praia[chave].observacoes = "Trechos impróprios: " + "; ".join(trechos)

        return list(por_praia.values())

    # ================================================================== #
    #  CAMADA 2 — PDF do INEA                                             #
    # ================================================================== #

    def _scrape_pdf_inea(self) -> List[BalneabilidadeData]:
        """
        1. Busca a URL do PDF mais recente na página do INEA
        2. Faz download do PDF em memória
        3. Extrai texto com pdfplumber
        4. Parseia praias e status
        """
        pdf_url = self._descobrir_url_pdf()
        if not pdf_url:
            logger.error("Não foi possível encontrar URL do PDF do INEA.")
            return []

        logger.info(f"Baixando PDF: {pdf_url}")
        resp = self._get(pdf_url)
        if not resp:
            logger.error("Falha ao baixar o PDF do INEA.")
            return []

        try:
            return self._parse_pdf(resp.content, pdf_url)
        except Exception as e:
            logger.error(f"Erro ao processar PDF: {e}")
            return []

    def _descobrir_url_pdf(self) -> Optional[str]:
        """
        Acessa a página de balneabilidade do INEA e extrai o link do PDF mais recente.
        O INEA sempre publica o PDF da semana com link no padrão:
          https://www.inea.rj.gov.br/wp-content/uploads/YYYY/MM/...pdf
        """
        resp = self._get(self.INEA_BALNEABILIDADE_URL)
        if not resp:
            return None

        soup = BeautifulSoup(resp.content, 'html.parser')

        # Procura todos os links que apontem para .pdf
        pdf_links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.lower().endswith('.pdf') and 'boletim' in href.lower():
                pdf_links.append(href)

        # Fallback: qualquer .pdf no domínio inea
        if not pdf_links:
            for a in soup.find_all('a', href=True):
                href = a['href']
                if href.lower().endswith('.pdf'):
                    pdf_links.append(href)

        if not pdf_links:
            logger.error("Nenhum link de PDF encontrado na página do INEA.")
            return None

        # Ordena por data no caminho (YYYY/MM) para pegar o mais recente
        pdf_links.sort(reverse=True)
        url = pdf_links[0]

        # Garante URL absoluta
        if url.startswith('/'):
            url = 'https://www.inea.rj.gov.br' + url

        logger.info(f"PDF encontrado: {url}")
        return url

    def _parse_pdf(self, conteudo: bytes, url_origem: str) -> List[BalneabilidadeData]:
        """
        Extrai dados de balneabilidade do PDF do INEA.
        O boletim lista praias com status 'PRÓPRIA' / 'IMPRÓPRIA'.
        """
        resultados: List[BalneabilidadeData] = []
        data_boletim: Optional[date] = None

        with pdfplumber.open(io.BytesIO(conteudo)) as pdf:
            texto_total = ''
            for pagina in pdf.pages:
                texto_total += (pagina.extract_text() or '') + '\n'

            # Extrai data do boletim (padrão: DD/MM/AAAA ou DD.MM.AAAA)
            data_boletim = self._extrair_data_pdf(texto_total)
            data_str = data_boletim.strftime('%Y-%m-%d') if data_boletim else datetime.now().strftime('%Y-%m-%d')

            logger.info(f"PDF: data do boletim = {data_str}")

            # Parseia linha a linha buscando padrões de praia + status
            # Padrões comuns no boletim INEA:
            #   "Copacabana  PRÓPRIA"
            #   "Ipanema (frente Rua X)  IMPRÓPRIA"
            #   "PRÓPRIA  Leblon"
            resultados = self._extrair_praias_do_texto(texto_total, data_str, url_origem)

        return resultados

    def _extrair_data_pdf(self, texto: str) -> Optional[date]:
        """Tenta extrair a data do boletim do texto do PDF."""
        padroes = [
            r'(\d{2})[/\.](\d{2})[/\.](\d{4})',
            r'(\d{4})-(\d{2})-(\d{2})',
        ]
        for padrao in padroes:
            for m in re.finditer(padrao, texto):
                try:
                    g = m.groups()
                    if len(g[0]) == 4:
                        return date(int(g[0]), int(g[1]), int(g[2]))
                    else:
                        return date(int(g[2]), int(g[1]), int(g[0]))
                except ValueError:
                    continue
        return None

    def _extrair_praias_do_texto(self, texto: str, data_str: str, url_origem: str) -> List[BalneabilidadeData]:
        resultados: List[BalneabilidadeData] = []
        # Normaliza o texto para evitar problemas de encoding/acentuação do PDF
        texto_limpo = texto.lower()

        status_encontrados = {}

        for praia_id, info in self.PRAIAS.items():
            nome_praia = info['nome'].lower()
            # Procura o nome da praia no texto
            if nome_praia in texto_limpo:
                # Busca o status mais próximo após o nome da praia
                pos = texto_limpo.find(nome_praia)
                # Pega os próximos 100 caracteres para buscar o status
                trecho = texto_limpo[pos:pos + 100]

                status = 'indisponivel'
                if any(p in trecho for p in ['imprópria', 'impropria']):
                    status = 'impropria'
                elif any(p in trecho for p in ['própria', 'propria']):
                    status = 'propria'

                if status != 'indisponivel':
                    status_encontrados[praia_id] = status

        # Monta objetos BalneabilidadeData
        for praia_id, status in status_encontrados.items():
            info = self.PRAIAS.get(praia_id, {})
            resultado = BalneabilidadeData(
                praia_id=praia_id,
                praia_nome=info.get('nome', praia_id),
                status=status,
                coliformes_fecais=None,
                data_coleta=data_str,
                municipio=info.get('municipio', ''),
                regiao=info.get('regiao', ''),
                coordenadas=info.get('coordenadas'),
                bairro=info.get('bairro', ''),
                extensao_km=info.get('extensao_km'),
                caracteristicas=info.get('caracteristicas', []),
                fonte="INEA-PDF",
                url_inea=url_origem,
            )
            resultados.append(resultado)

        logger.info(f"PDF: {len(resultados)} praias extraídas.")
        return resultados

    # ================================================================== #
    #  UTILITÁRIOS                                                         #
    # ================================================================== #

    def _determinar_status(self, texto: str) -> str:
        t = texto.lower().strip()
        if any(p in t for p in ['própria', 'propria', 'adequada', 'boa']):
            return 'propria'
        if any(p in t for p in ['imprópria', 'impropria', 'inadequada', 'ruim']):
            return 'impropria'
        return 'indisponivel'

    def _identificar_praia(self, texto: str) -> Tuple[Optional[str], Optional[Dict]]:
        """Identifica praia do cadastro por correspondência fuzzy no texto."""
        texto_lower = texto.lower()
        melhor_id = None
        melhor_info = None
        max_score = 0

        for pid, info in self.PRAIAS.items():
            score = 0
            nome_lower = info['nome'].lower()

            if nome_lower in texto_lower:
                score += 10
            elif pid.replace('_', ' ') in texto_lower:
                score += 8
            else:
                for parte in nome_lower.split():
                    if len(parte) > 3 and parte in texto_lower:
                        score += 3

            if score > max_score:
                max_score = score
                melhor_info = info
                melhor_id = pid

#mudei o max_score de 8 para 5
        if max_score >= 5:
            return melhor_id, melhor_info
        return None, None

    def _inferir_regiao(self, municipio: str) -> str:
        mapa = {
            'Rio de Janeiro': 'Zona Sul',
            'Niterói': 'Niterói',
            'Angra dos Reis': 'Costa Verde',
            'Paraty': 'Costa Verde',
            'Mangaratiba': 'Costa Verde',
            'Cabo Frio': 'Região dos Lagos',
            'Búzios': 'Região dos Lagos',
            'Armação dos Búzios': 'Região dos Lagos',
            'Saquarema': 'Região dos Lagos',
            'Araruama': 'Região dos Lagos',
            'Arraial do Cabo': 'Região dos Lagos',
        }
        return mapa.get(municipio, 'Outro')

    def _slugify(self, nome: str) -> str:
        return re.sub(r'[^a-z0-9_]', '_', nome.lower().strip()).strip('_')

    def _extrair_coliformes(self, texto: str) -> Optional[int]:
        m = re.search(r'(\d[\d\.,]*)\s*(NMP|UFC|col)?', texto, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1).replace('.', '').replace(',', ''))
            except ValueError:
                pass
        return None

    # ------------------------------------------------------------------ #
    #  FILTROS                                                             #
    # ------------------------------------------------------------------ #

    def filtrar_por_regiao(self, dados, regiao):
        return [d for d in dados if d.regiao.lower() == regiao.lower()]

    def filtrar_proprias(self, dados):
        return [d for d in dados if d.status == 'propria']

    def praias_proximas(self, dados, latitude, longitude, raio_km=10.0):
        from math import radians, cos, sin, asin, sqrt
        def haversine(lat1, lon1, lat2, lon2):
            R = 6371
            dlat = radians(lat2 - lat1)
            dlon = radians(lon2 - lon1)
            a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
            return 2 * R * asin(sqrt(a))
        resultado = []
        for d in dados:
            if d.coordenadas:
                dist = haversine(latitude, longitude, d.coordenadas['latitude'], d.coordenadas['longitude'])
                if dist <= raio_km:
                    resultado.append((dist, d))
        resultado.sort(key=lambda x: x[0])
        return [d for _, d in resultado]

    # ------------------------------------------------------------------ #
    #  EXPORTAÇÃO                                                          #
    # ------------------------------------------------------------------ #

    def exportar_json(self, dados, arquivo='balneabilidade.json'):
        with open(arquivo, 'w', encoding='utf-8') as f:
            json.dump([asdict(d) for d in dados], f, ensure_ascii=False, indent=2)
        logger.info(f"✅ JSON exportado: {arquivo} ({len(dados)} registros)")

    def obter_estatisticas(self, dados):
        total = len(dados)
        proprias = sum(1 for d in dados if d.status == 'propria')
        improprias = sum(1 for d in dados if d.status == 'impropria')
        return {
            'total_praias': total,
            'praias_proprias': proprias,
            'praias_improprias': improprias,
            'praias_indisponiveis': total - proprias - improprias,
            'percentual_proprias': round((proprias / total * 100) if total > 0 else 0, 1),
            'data_referencia': datetime.now().strftime('%Y-%m-%d %H:%M'),
        }


# ------------------------------------------------------------------ #
#  MAIN                                                                #
# ------------------------------------------------------------------ #

def main():
    scraper = INEAScraper()
    try:
        dados = scraper.scrape_balneabilidade()
    except RuntimeError as e:
        print(f"\n❌ ERRO: {e}")
        return

    print(f"\n✅ {len(dados)} praias coletadas | fonte: {dados[0].fonte if dados else 'n/a'}")
    for d in sorted(dados, key=lambda x: x.praia_nome):
        emoji = "🟢" if d.status == 'propria' else ("🔴" if d.status == 'impropria' else "⚫")
        obs = f" — {d.observacoes}" if d.observacoes else ""
        print(f"  {emoji} {d.praia_nome:<30} {d.status.upper():<12} {d.data_coleta}{obs}")

    stats = scraper.obter_estatisticas(dados)
    print(f"\n📊 Próprias: {stats['praias_proprias']}/{stats['total_praias']} ({stats['percentual_proprias']}%)")


if __name__ == "__main__":
    main()
