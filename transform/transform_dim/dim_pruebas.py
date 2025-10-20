import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['pruebas','hitos']

def transform(df_dict: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_pruebas
    Output sugerido: ['ID_Prueba','CodigoPrueba','ID_Hito','TipoPrueba','PruebaExitosa']
    """
    pruebas = df_dict.get('pruebas', pd.DataFrame())
    if pruebas.empty:
        return pd.DataFrame(columns=['ID_Prueba','CodigoPrueba','ID_Hito','TipoPrueba','PruebaExitosa'])

    df = pruebas.copy()
    df['CodigoPrueba'] = df['ID_Prueba']
    result = df[['ID_Prueba','CodigoPrueba','ID_Hito','TipoPrueba','Exitosa']]
    result = result.rename(columns={'Exitosa':'PruebaExitosa'})
    logger.info(f"dim_pruebas: preparado {len(result)} filas")
    return result
