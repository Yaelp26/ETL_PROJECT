import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['proyectos','contratos']

def transform(df_dict: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_proyectos
    Output columns (sugeridas): ['ID_Proyecto','CodigoProyecto','Version','Cancelado','ID_Cliente']
    """
    proyectos = df_dict.get('proyectos', pd.DataFrame())
    if proyectos.empty:
        logger.warning('dim_proyectos: df proyectos vacío')
        return pd.DataFrame(columns=['ID_Proyecto','CodigoProyecto','Version','Cancelado','ID_Cliente'])

    df = proyectos.copy()
    df['CodigoProyecto'] = df['ID_Proyecto']
    df['Cancelado'] = df.get('EstadoProyecto', '').apply(lambda x: 1 if x == 'Cancelado' else 0) if 'EstadoProyecto' in df else 0

    result = df[['ID_Proyecto','CodigoProyecto','Version','Cancelado','ID_Cliente']]
    logger.info(f"dim_proyectos: preparado {len(result)} filas")
    return result
