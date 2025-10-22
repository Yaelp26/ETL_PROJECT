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
    tareas = ensure_df(df_dict.get('tareas', pd.DataFrame()))
    
    if tareas.empty:
        logger.warning('dim_tareas: No hay datos de tareas')
        return pd.DataFrame(columns=['ID_Tarea','CodigoTarea','ID_Hito','DuracionDias','RetrasoDias'])

    df = tareas.copy()
    df['CodigoTarea'] = df['ID_Tarea']
    
    # DuracionDias = DuracionReal del SGP
    df['DuracionDias'] = pd.to_numeric(df['DuracionReal'], errors='coerce').fillna(0).astype(int)
    
    # RetrasoDias = DuracionReal - DuracionPlanificada
    duracion_real = pd.to_numeric(df['DuracionReal'], errors='coerce').fillna(0)
    duracion_planificada = pd.to_numeric(df['DuracionPlanificada'], errors='coerce').fillna(0)
    df['RetrasoDias'] = (duracion_real - duracion_planificada).astype(int)

    result = df[['ID_Tarea','CodigoTarea','ID_Hito','DuracionDias','RetrasoDias']]
    
    log_transform_info('dim_tareas', len(tareas), len(result))
    return result