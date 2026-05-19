import copernicusmarine
from datetime import datetime, timedelta
import xarray as xr
import pandas as pd

# --- Configurações ---
SST_DATASET_ID = "cmems_mod_glo_phy_anfc_0.083deg_P1D-m"
WAVES_DATASET_ID = "cmems_mod_glo_wav_anfc_0.083deg_PT3H-i"

date_of_interest = datetime.now() - timedelta(days=1)
start_date_str = date_of_interest.strftime('%Y-%m-%d')
end_date_str = (date_of_interest + timedelta(days=1)).strftime('%Y-%m-%d')

# Coordenadas da Bacia de Campos/RJ (ajuste conforme sua necessidade)
LONGITUDE = [-43.8, -42.8]
LATITUDE = [-23.1, -22.8]

print(f"🔄 Iniciando a coleta para a data: {start_date_str}")

# --- Busca por Temperatura da Água (thetao) ---
try:
    print("🌊 Buscando dados de Temperatura da Água...")
    sst_subset = copernicusmarine.subset(
        dataset_id=SST_DATASET_ID,
        variables=["tob"],                     # <--- Correção principal
        minimum_longitude=LONGITUDE[0],
        maximum_longitude=LONGITUDE[1],
        minimum_latitude=LATITUDE[0],
        maximum_latitude=LATITUDE[1],
        start_datetime=start_date_str,
        end_datetime=end_date_str,
        minimum_depth=0.49,
        maximum_depth=0.49,
    )
    print("✅ Download dos dados de Temperatura concluído.")
except Exception as e:
    print(f"❌ Erro ao baixar Temperatura: {e}")

# --- Busca por Dados de Ondas (VHM0) ---
# Nota: O dataset de ondas tem resolução temporal de 3 horas (PT3H).
print("🌊 Buscando dados de Ondas...")
waves_subset = copernicusmarine.subset(
    dataset_id=WAVES_DATASET_ID,
    variables=["VMDR_SW1"],                           # <--- Correção principal
    minimum_longitude=LONGITUDE[0],
    maximum_longitude=LONGITUDE[1],
    minimum_latitude=LATITUDE[0],
    maximum_latitude=LATITUDE[1],
    start_datetime=start_date_str,
    end_datetime=end_date_str,
    # Profundidade omitida para datasets 2D
)

print("✅ Coleta finalizada!")