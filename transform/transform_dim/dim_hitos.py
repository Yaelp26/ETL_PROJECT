"""
Transformación dim_hitos - Proyecto Escolar ETL
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
    return ['hitos', 'proyectos']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_hitos"""
    hitos = ensure_df(df_dict.get('hitos', pd.DataFrame()))
    
    if hitos.empty:
        logger.warning('dim_hitos: No hay datos de hitos')
        return pd.DataFrame(columns=['ID_Hito','CodigoHito','ID_proyectos','ID_FechaInicio','ID_FechaFinalizacion','Retraso_days'])

    df = hitos.copy()
    df['CodigoHito'] = df['ID_Hito']
    
    # Convertir fechas a string para ID_Fecha (simplificado para proyecto escolar)
    df['ID_FechaInicio'] = df['FechaInicio'].astype(str)
    df['ID_FechaFinalizacion'] = df['FechaFinReal'].astype(str)
    
    # Usar días de retraso calculados o 0
    df['Retraso_days'] = df.get('dias_retraso', 0).fillna(0).astype(int)

    result = df[['ID_Hito','CodigoHito','ID_Proyecto','ID_FechaInicio','ID_FechaFinalizacion','Retraso_days']]
    result = result.rename(columns={'ID_Proyecto':'ID_proyectos'})
    
    log_transform_info('dim_hitos', len(hitos), len(result))
    return result

def test_transform():
    sample_data = {
        'hitos': pd.DataFrame({
            'ID_Hito': [1, 2, 3],
            'ID_Proyecto': [1, 1, 2],
            'FechaInicio': ['2023-01-15', '2023-03-01', '2023-02-01'],
            'FechaFinReal': ['2023-02-15', '2023-04-01', '2023-03-15'],
            'dias_retraso': [5, 0, -2]
        })
    }
    result = transform(sample_data)
    print("Test dim_hitos:")
    print(result)
    return result

if __name__ == "__main__":
    test_transform()
