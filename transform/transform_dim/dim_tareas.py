"""
Transformación dim_tareas - Proyecto Escolar ETL
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
    return ['tareas', 'hitos']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_tareas"""
    tareas = ensure_df(df_dict.get('tareas', pd.DataFrame()))
    
    if tareas.empty:
        logger.warning('dim_tareas: No hay datos de tareas')
        return pd.DataFrame(columns=['ID_Tarea','CodigoTarea','ID_Hito','DuracionDias','RetrasoDias'])

    df = tareas.copy()
    df['CodigoTarea'] = df['ID_Tarea']
    
    # Asegurar tipos numéricos
    df['DuracionDias'] = pd.to_numeric(df['DuracionPlanificada'], errors='coerce').fillna(0).astype(int)
    df['RetrasoDias'] = pd.to_numeric(df.get('desviacion_duracion', 0), errors='coerce').fillna(0).astype(int)

    result = df[['ID_Tarea','CodigoTarea','ID_Hito','DuracionDias','RetrasoDias']]
    
    log_transform_info('dim_tareas', len(tareas), len(result))
    return result

def test_transform():
    sample_data = {
        'tareas': pd.DataFrame({
            'ID_Tarea': [1, 2, 3],
            'ID_Hito': [1, 1, 2],
            'DuracionPlanificada': [10, 15, 8],
            'desviacion_duracion': [2, 0, -1]
        })
    }
    result = transform(sample_data)
    print("Test dim_tareas:")
    print(result)
    return result

if __name__ == "__main__":
    test_transform()
