"""
Transformación dim_pruebas - Proyecto Escolar ETL
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
    return ['pruebas', 'hitos']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_pruebas"""
    pruebas = ensure_df(df_dict.get('pruebas', pd.DataFrame()))
    
    if pruebas.empty:
        logger.warning('dim_pruebas: No hay datos de pruebas')
        return pd.DataFrame(columns=['ID_Prueba','CodigoPrueba','ID_Hito','TipoPrueba','PruebaExitosa'])

    df = pruebas.copy()
    df['CodigoPrueba'] = df['ID_Prueba']
    
    # Normalizar resultado de prueba
    df['PruebaExitosa'] = df['Exitosa'].fillna(0).astype(int)
    
    # Limpiar tipo de prueba
    df['TipoPrueba'] = df['TipoPrueba'].astype(str).str.strip()

    result = df[['ID_Prueba','CodigoPrueba','ID_Hito','TipoPrueba','PruebaExitosa']]
    
    log_transform_info('dim_pruebas', len(pruebas), len(result))
    return result

def test_transform():
    sample_data = {
        'pruebas': pd.DataFrame({
            'ID_Prueba': [1, 2, 3, 4],
            'ID_Hito': [1, 1, 2, 2],
            'TipoPrueba': ['Unitaria', 'Integración', 'Aceptación', 'Unitaria'],
            'Exitosa': [1, 1, 0, 1]
        })
    }
    result = transform(sample_data)
    print("Test dim_pruebas:")
    print(result)
    return result

if __name__ == "__main__":
    test_transform()
