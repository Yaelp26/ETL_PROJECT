"""
Transformación dim_riesgos - Proyecto Escolar ETL
"""
import pandas as pd
import logging
from typing import Dict
import sys
import os

# Agregar path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from transform.common import ensure_df, log_transform_info

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['riesgos']

def create_mapping_dictionaries(riesgos_df: pd.DataFrame) -> tuple:
    """Crea diccionarios de mapeo para tipos y severidades"""
    # Mapeo tipos de riesgo
    tipos_unicos = riesgos_df['TipoRiesgo'].dropna().unique()
    tipo_map = {tipo: idx + 1 for idx, tipo in enumerate(tipos_unicos)}
    
    # Mapeo severidades
    severidades_unicas = riesgos_df['Severidad'].dropna().unique()
    severidad_map = {sev: idx + 1 for idx, sev in enumerate(severidades_unicas)}
    
    return tipo_map, severidad_map

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_riesgos"""
    riesgos = ensure_df(df_dict.get('riesgos', pd.DataFrame()))
    
    if riesgos.empty:
        logger.warning('dim_riesgos: No hay datos de riesgos')
        return pd.DataFrame(columns=['ID_Riesgo', 'ID_TipoRiesgo', 'ID_Severidad'])

    # Crear mapeos
    tipo_map, severidad_map = create_mapping_dictionaries(riesgos)
    
    df = riesgos.copy()
    
    # Mapear tipos y severidades a IDs
    df['ID_TipoRiesgo'] = df['TipoRiesgo'].map(tipo_map).fillna(0).astype(int)
    df['ID_Severidad'] = df['Severidad'].map(severidad_map).fillna(0).astype(int)
    
    # Seleccionar columnas finales
    result = df[['ID_Riesgo', 'ID_TipoRiesgo', 'ID_Severidad']]
    
    log_transform_info('dim_riesgos', len(riesgos), len(result))
    return result

def test_transform():
    sample_data = {
        'riesgos': pd.DataFrame({
            'ID_Riesgo': [1, 2, 3, 4],
            'TipoRiesgo': ['Técnico', 'Operativo', 'Financiero', 'Técnico'],
            'Severidad': ['Alta', 'Media', 'Crítica', 'Baja']
        })
    }
    result = transform(sample_data)
    print("Test dim_riesgos:")
    print(result)
    return result

if __name__ == "__main__":
    test_transform()
