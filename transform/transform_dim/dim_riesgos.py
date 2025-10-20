import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['riesgos','dim_tipo_riesgo','dim_severidad']

def transform(df_dict: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_riesgos
    Output: ['ID_Riesgo','ID_TipoRiesgo','ID_Severidad']
    """
    riesgos = df_dict.get('riesgos', pd.DataFrame())
    if riesgos.empty:
        return pd.DataFrame(columns=['ID_Riesgo','ID_TipoRiesgo','ID_Severidad'])

    df = riesgos.copy()
    # Placeholder: mapear tipos/severidad a IDs después
    df['ID_TipoRiesgo'] = None
    df['ID_Severidad'] = None
    result = df[['ID_Riesgo','ID_TipoRiesgo','ID_Severidad']]
    logger.info(f"dim_riesgos: preparado {len(result)} filas (esqueleto)")
    return result
