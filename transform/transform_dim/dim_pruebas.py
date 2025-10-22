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
    pruebas = ensure_df(df_dict.get('pruebas', pd.DataFrame()))
    hitos = ensure_df(df_dict.get('hitos', pd.DataFrame()))
    
    if pruebas.empty:
        logger.warning('dim_pruebas: No hay datos de pruebas')
        return pd.DataFrame(columns=['ID_Prueba','CodigoPrueba','ID_Hito','TipoPrueba','PruebaExitosa'])

    df = pruebas.copy()
    df['CodigoPrueba'] = df['ID_Prueba']
    
    # Normalizar resultado de prueba
    df['PruebaExitosa'] = df['Exitosa'].fillna(0).astype(int)
    
    # Limpiar tipo de prueba
    df['TipoPrueba'] = df['TipoPrueba'].astype(str).str.strip()

    # ===== VALIDACIÓN FK: ID_Hito → dim_hitos =====
    if not hitos.empty:
        valid_hitos = set(hitos['ID_Hito'].unique())
        initial_count = len(df)
        
        # Filtrar pruebas con hitos válidos
        df = df[df['ID_Hito'].isin(valid_hitos)]
        
        filtered_count = initial_count - len(df)
        if filtered_count > 0:
            logger.info(f'dim_pruebas: Filtradas {filtered_count} pruebas con hitos inexistentes')
    else:
        logger.warning('dim_pruebas: No hay datos de hitos para validar FK')

    result = df[['ID_Prueba','CodigoPrueba','ID_Hito','TipoPrueba','PruebaExitosa']]
    
    log_transform_info('dim_pruebas', len(pruebas), len(result))
    return result
