# -*- coding: utf-8 -*-
"""
Created on Thu Feb 19 13:33:01 2026

@author: Delano
"""

#!/usr/bin/env python3
"""
INEA Balneabilidade Scraper
Sistema de coleta automática de dados de balneabilidade das praias do Rio de Janeiro

Autor: Sistema Praias RJ
Versão: 1.0
"""

import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import time
from dataclasses import dataclass, asdict

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
class BalneabilidadeData:
    """Estrutura de dados para balneabilidade"""
    praia_id: str
    praia_nome: str
    status: str  # 'propria' | 'impropria' | 'indisponivel'
    coliformes_fecais: Optional[int]
    data_coleta: str  # formato: YYYY-MM-DD
    municipio: str
    regiao: str
    fonte: str = "INEA"
    observacoes: Optional[str] = None
    timestamp: str = datetime.now().isoformat()


class INEAScraper:
    """Scraper para dados de balneabilidade do INEA"""
    
    BASE_URL = "http://www.inea.rj.gov.br"
    BALNEABILIDADE_URL = "http://200.20.53.17/"
    
    # 15 praias principais do Rio
    PRAIAS_RIO = {
        "copacabana": {"nome": "Copacabana", "regiao": "Zona Sul"},
        "ipanema": {"nome": "Ipanema", "regiao": "Zona Sul"},
        "leblon": {"nome": "Leblon", "regiao": "Zona Sul"},
        "arpoador": {"nome": "Arpoador", "regiao": "Zona Sul"},
        "leme": {"nome": "Leme", "regiao": "Zona Sul"},
        "barra": {"nome": "Barra da Tijuca", "regiao": "Zona Oeste"},
        "recreio": {"nome": "Recreio dos Bandeirantes", "regiao": "Zona Oeste"},
        "grumari": {"nome": "Grumari", "regiao": "Zona Oeste"},
        "prainha": {"nome": "Prainha", "regiao": "Zona Oeste"},
        "joatinga": {"nome": "Joatinga", "regiao": "Zona Oeste"},
        "sao_conrado": {"nome": "São Conrado", "regiao": "Zona Sul"},
        "flamengo": {"nome": "Flamengo", "regiao": "Zona Sul"},
        "botafogo": {"nome": "Botafogo", "regiao": "Zona Sul"},
        "urca": {"nome": "Urca", "regiao": "Zona Sul"},
        "vermelha": {"nome": "Vermelha", "regiao": "Zona Sul"},
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
        
    def _fazer_requisicao(self, url: str) -> Optional[requests.Response]:
        """Faz requisição HTTP com retry automático"""
        for tentativa in range(self.retry_attempts):
            try:
                logger.info(f"Requisição: {url} (tentativa {tentativa + 1})")
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Erro: {str(e)}")
                if tentativa < self.retry_attempts - 1:
                    time.sleep(2 ** tentativa)
        return None
    
    def _determinar_status(self, texto_status: str, coliformes: Optional[int] = None) -> str:
        """Determina status da praia"""
        texto = texto_status.lower().strip()
        
        if 'própria' in texto or 'propria' in texto:
            return 'propria'
        elif 'imprópria' in texto or 'impropria' in texto:
            return 'impropria'
        
        if coliformes is not None:
            return 'propria' if coliformes < 800 else 'impropria'
        
        return 'indisponivel'
    
    def scrape_balneabilidade(self) -> List[BalneabilidadeData]:
        """Coleta dados de balneabilidade"""
        logger.info("Iniciando coleta de dados")
        
        # Tentar scraping real
        response = self._fazer_requisicao(self.BALNEABILIDADE_URL)
        if response:
            resultados = self._parse_html(response)
            if resultados:
                logger.info(f"✅ Coletados {len(resultados)} praias")
                return resultados
        
        # Fallback: dados mockados para desenvolvimento
        logger.warning("Usando dados mockados")
        return self._gerar_dados_mockados()
    
    def _parse_html(self, response: requests.Response) -> List[BalneabilidadeData]:
        """Parse HTML do INEA"""
        soup = BeautifulSoup(response.content, 'html.parser')
        resultados = []
        
        tabelas = soup.find_all('table')
        for tabela in tabelas:
            linhas = tabela.find_all('tr')
            for linha in linhas[1:]:
                colunas = linha.find_all('td')
                if len(colunas) >= 2:
                    try:
                        nome = colunas[0].get_text(strip=True)
                        status_texto = colunas[1].get_text(strip=True)
                        
                        # Identificar praia
                        praia_id = None
                        praia_info = None
                        nome_lower = nome.lower()
                        
                        for pid, info in self.PRAIAS_RIO.items():
                            if info['nome'].lower() in nome_lower or pid in nome_lower:
                                praia_id = pid
                                praia_info = info
                                break
                        
                        if not praia_info:
                            continue
                        
                        resultado = BalneabilidadeData(
                            praia_id=praia_id,
                            praia_nome=praia_info['nome'],
                            status=self._determinar_status(status_texto),
                            coliformes_fecais=None,
                            data_coleta=datetime.now().strftime('%Y-%m-%d'),
                            municipio="Rio de Janeiro",
                            regiao=praia_info['regiao']
                        )
                        resultados.append(resultado)
                        
                    except Exception as e:
                        logger.warning(f"Erro ao processar linha: {e}")
        
        return resultados
    
    def _gerar_dados_mockados(self) -> List[BalneabilidadeData]:
        """Gera dados de teste (REMOVER EM PRODUÇÃO)"""
        import random
        resultados = []
        
        for praia_id, info in self.PRAIAS_RIO.items():
            status = 'propria' if random.random() > 0.2 else 'impropria'
            coliformes = random.randint(200, 700) if status == 'propria' else random.randint(800, 1500)
            
            resultado = BalneabilidadeData(
                praia_id=praia_id,
                praia_nome=info['nome'],
                status=status,
                coliformes_fecais=coliformes,
                data_coleta=datetime.now().strftime('%Y-%m-%d'),
                municipio="Rio de Janeiro",
                regiao=info['regiao'],
                fonte="MOCK_DATA",
                observacoes="⚠️ Dados de teste"
            )
            resultados.append(resultado)
        
        return resultados
    
    def exportar_json(self, dados: List[BalneabilidadeData], arquivo: str = 'balneabilidade.json'):
        """Exporta para JSON"""
        dados_dict = [asdict(d) for d in dados]
        with open(arquivo, 'w', encoding='utf-8') as f:
            json.dump(dados_dict, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ Exportado: {arquivo}")
    
    def obter_estatisticas(self, dados: List[BalneabilidadeData]) -> Dict:
        """Calcula estatísticas"""
        total = len(dados)
        proprias = sum(1 for d in dados if d.status == 'propria')
        improprias = sum(1 for d in dados if d.status == 'impropria')
        
        return {
            'total_praias': total,
            'praias_proprias': proprias,
            'praias_improprias': improprias,
            'percentual_proprias': round((proprias / total * 100) if total > 0 else 0, 1)
        }


def main():
    """Função principal"""
    print("="*60)
    print("INEA BALNEABILIDADE SCRAPER")
    print("="*60)
    
    scraper = INEAScraper()
    dados = scraper.scrape_balneabilidade()
    
    # Mostrar resultados
    print(f"\n✅ {len(dados)} praias coletadas\n")
    for dado in dados:
        emoji = "🟢" if dado.status == 'propria' else "🔴"
        print(f"{emoji} {dado.praia_nome:25} {dado.status.upper():12} ({dado.regiao})")
    
    # Estatísticas
    stats = scraper.obter_estatisticas(dados)
    print(f"\n📊 ESTATÍSTICAS:")
    print(f"Total: {stats['total_praias']}")
    print(f"Próprias: {stats['praias_proprias']} ({stats['percentual_proprias']}%)")
    print(f"Impróprias: {stats['praias_improprias']}")
    
    # Exportar
    scraper.exportar_json(dados)
    print("\n✅ Dados salvos em balneabilidade.json")


if __name__ == "__main__":
    main()