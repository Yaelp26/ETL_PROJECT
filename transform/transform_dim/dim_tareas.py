import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['tareas','hitos']

def transform(df_dict: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_tareas
    Output sugerido: ['ID_Tarea','CodigoTarea','ID_Hito','DuracionDias','RetrasoDias']
    """
    tareas = df_dict.get('tareas', pd.DataFrame())
    if tareas.empty:
        return pd.DataFrame(columns=['ID_Tarea','CodigoTarea','ID_Hito','DuracionDias','RetrasoDias'])

    df = tareas.copy()
    df['CodigoTarea'] = df['ID_Tarea']
    result = df[['ID_Tarea','CodigoTarea','ID_Hito','DuracionPlanificada','desviacion_duracion']]
    result = result.rename(columns={'DuracionPlanificada':'DuracionDias','desviacion_duracion':'RetrasoDias'})
    logger.info(f"dim_tareas: preparado {len(result)} filas")
    return result
