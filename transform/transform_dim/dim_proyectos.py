"""
Transformación dim_proyectos - Proyecto Escolar ETL
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
    return ['proyectos', 'contratos']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_proyectos"""
    proyectos = ensure_df(df_dict.get('proyectos', pd.DataFrame()))
    
    if proyectos.empty:
        logger.warning('dim_proyectos: No hay datos de proyectos')
        return pd.DataFrame(columns=['ID_Proyecto','CodigoProyecto','Version','Cancelado','ID_Cliente'])

    df = proyectos.copy()
    df['CodigoProyecto'] = df['ID_Proyecto']
    
    # Determinar si está cancelado
    if 'EstadoProyecto' in df.columns:
        df['Cancelado'] = df['EstadoProyecto'].apply(lambda x: 1 if str(x).upper() == 'CANCELADO' else 0)
    else:
        df['Cancelado'] = 0
    
    # Limpiar versión
    df['Version'] = df['Version'].fillna('1.0').astype(str).str.strip()

    result = df[['ID_Proyecto','CodigoProyecto','Version','Cancelado','ID_Cliente']]
    log_transform_info('dim_proyectos', len(proyectos), len(result))
    return result

def test_transform():
    sample_data = {
        'proyectos': pd.DataFrame({
            'ID_Proyecto': [1, 2, 3],
            'ID_Cliente': [1, 1, 2],
            'Version': ['1.0', '2.1', None],
            'EstadoProyecto': ['Terminado', 'Cancelado', 'Terminado']
        })
    }
    result = transform(sample_data)
    print("Test dim_proyectos:")
    print(result)
    return result

if __name__ == "__main__":
    test_transform()
