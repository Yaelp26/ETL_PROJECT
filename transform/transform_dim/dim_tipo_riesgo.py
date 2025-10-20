import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['riesgos']

def transform(df_dict: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_tipo_riesgo
    Output: ['ID_TipoRiesgo','NombreTipo']
    """
    riesgos = df_dict.get('riesgos', pd.DataFrame())
    if riesgos.empty:
        return pd.DataFrame(columns=['ID_TipoRiesgo','NombreTipo'])

    s = riesgos['TipoRiesgo'].dropna().unique()
    df = pd.DataFrame({'NombreTipo': s})
    df.insert(0,'ID_TipoRiesgo', range(1, len(df)+1))
    logger.info(f'dim_tipo_riesgo: creado {len(df)} tipos')
    return df
