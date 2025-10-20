import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['gastos','penalizaciones']

def transform(df_dict: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_finanzas
    Output columns sugeridas: ['ID_Finanza','TipoGasto','Categoria','Monto']
    """
    gastos = df_dict.get('gastos', pd.DataFrame())
    penal = df_dict.get('penalizaciones', pd.DataFrame())

    # Combinar tipos para la dimensión
    df_g = gastos.copy() if not gastos.empty else pd.DataFrame(columns=['TipoGasto','Categoria','Monto'])
    df_p = penal.copy() if not penal.empty else pd.DataFrame(columns=['Monto'])

    # Esqueleto simple
    cols = ['ID_Finanza','TipoGasto','Categoria','Monto']
    logger.info('dim_finanzas: esqueleto creado')
    return pd.DataFrame(columns=cols)
