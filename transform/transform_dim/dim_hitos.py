import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['hitos','proyectos']

def transform(df_dict: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_hitos
    Output sugerido: ['ID_Hito','CodigoHito','ID_proyectos','ID_FechaInicio','ID_FechaFinalizacion','Retraso_days']
    """
    hitos = df_dict.get('hitos', pd.DataFrame())
    if hitos.empty:
        return pd.DataFrame(columns=['ID_Hito','CodigoHito','ID_proyectos','ID_FechaInicio','ID_FechaFinalizacion','Retraso_days'])

    df = hitos.copy()
    df['CodigoHito'] = df['ID_Hito']
    result = df[['ID_Hito','CodigoHito','ID_Proyecto','FechaInicio','FechaFinReal','dias_retraso']]
    result = result.rename(columns={'ID_Proyecto':'ID_proyectos','FechaInicio':'ID_FechaInicio','FechaFinReal':'ID_FechaFinalizacion','dias_retraso':'Retraso_days'})
    logger.info(f"dim_hitos: preparado {len(result)} filas")
    return result
