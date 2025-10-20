import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['riesgos']

def transform(df_dict: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_severidad
    Output: ['ID_Severidad','Nivel']
    """
    riesgos = df_dict.get('riesgos', pd.DataFrame())
    if riesgos.empty:
        return pd.DataFrame(columns=['ID_Severidad','Nivel'])

    s = riesgos['Severidad'].dropna().unique()
    df = pd.DataFrame({'Nivel': s})
    df.insert(0,'ID_Severidad', range(1, len(df)+1))
    logger.info(f'dim_severidad: creado {len(df)} niveles')
    return df
