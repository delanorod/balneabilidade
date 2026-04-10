# -*- coding: utf-8 -*-
"""
inicializar_praias.py

Roda UMA VEZ para criar o praias_rj.json com lat/lon de cada praia.
Coordenadas verificadas via Google Maps — sem dependência de API externa.
Depois disso, gerar_json_praias.py já funciona normalmente.
"""

import json
from datetime import datetime

PRAIAS = [
    # Zona Sul
    {"nome": "Leme",                     "lat": -22.9647, "lon": -43.1726},
    {"nome": "Copacabana",               "lat": -22.9711, "lon": -43.1823},
    {"nome": "Ipanema",                  "lat": -22.9838, "lon": -43.2046},
    {"nome": "Leblon",                   "lat": -22.9867, "lon": -43.2234},
    {"nome": "Vidigal",                  "lat": -22.9944, "lon": -43.2407},
    {"nome": "Sao Conrado",              "lat": -23.0103, "lon": -43.2795},
    # Barra e Recreio
    {"nome": "Barra da Tijuca",          "lat": -23.0108, "lon": -43.3654},
    {"nome": "Recreio dos Bandeirantes", "lat": -23.0182, "lon": -43.4665},
    {"nome": "Macumba",                  "lat": -23.0228, "lon": -43.4908},
    {"nome": "Prainha",                  "lat": -23.0369, "lon": -43.5163},
    {"nome": "Grumari",                  "lat": -23.0449, "lon": -43.5371},
    # Zona Norte / Baia
    {"nome": "Flamengo",                 "lat": -22.9313, "lon": -43.1731},
    {"nome": "Botafogo",                 "lat": -22.9519, "lon": -43.1823},
    {"nome": "Ramos",                    "lat": -22.8558, "lon": -43.2102},
    # Niteroi
    {"nome": "Icarai",                   "lat": -22.9027, "lon": -43.1123},
    {"nome": "Jurujuba",                 "lat": -22.9219, "lon": -43.0891},
    {"nome": "Piratininga",              "lat": -22.9604, "lon": -43.0541},
    {"nome": "Camboinhas",               "lat": -22.9690, "lon": -43.0388},
    {"nome": "Itacoatiara",              "lat": -22.9656, "lon": -43.0219},
]

def inicializar():
    print("=" * 55)
    print(f"Inicializando praias_rj.json com {len(PRAIAS)} praias...")
    print("=" * 55)

    praias_final = []
    for p in PRAIAS:
        print(f"  OK  {p['nome']:<30} lat={p['lat']}, lon={p['lon']}")
        praias_final.append({
            "nome":           p["nome"],
            "lat":            p["lat"],
            "lon":            p["lon"],
            "onda":           None,
            "vento":          None,
            "agitacao":       None,
            "direcao":        None,
            "balneabilidade": None,
            "regiao":         None,
            "score":          0,
        })

    json_final = {
        "ultima_atualizacao":   datetime.now().isoformat(),
        "fonte_ondas":          None,
        "fonte_balneabilidade": None,
        "praia_recomendada":    None,
        "praias":               praias_final,
    }

    with open("praias_rj.json", "w", encoding="utf-8") as f:
        json.dump(json_final, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 55)
    print(f"praias_rj.json criado com {len(praias_final)} praias.")
    print("Agora rode: python gerar_json_praias.py")

if __name__ == "__main__":
    inicializar()
