"""
Transformación dim_severidad - Proyecto Escolar ETL
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

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_severidad"""
    riesgos = ensure_df(df_dict.get('riesgos', pd.DataFrame()))
    
    if riesgos.empty:
        logger.warning('dim_severidad: No hay datos de riesgos')
        return pd.DataFrame(columns=['ID_Severidad', 'Nivel'])

    # Extraer niveles únicos
    niveles_unicos = riesgos['Severidad'].dropna().unique()
    
    if len(niveles_unicos) == 0:
        logger.warning('dim_severidad: No hay niveles de severidad válidos')
        return pd.DataFrame(columns=['ID_Severidad', 'Nivel'])
    
    # Crear dimensión
    result = pd.DataFrame({
        'ID_Severidad': range(1, len(niveles_unicos) + 1),
        'Nivel': niveles_unicos
    })
    
    # Limpiar datos
    result['Nivel'] = result['Nivel'].astype(str).str.strip()
    
    log_transform_info('dim_severidad', len(riesgos), len(result))
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
    print("Test dim_severidad:")
    print(result)
    return result

if __name__ == "__main__":
    test_transform()
