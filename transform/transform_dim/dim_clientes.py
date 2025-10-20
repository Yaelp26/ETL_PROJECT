import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['clientes','contratos']

def transform(df_dict: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_clientes
    Input: df_dict con claves 'clientes' y 'contratos'
    Output: DataFrame con columnas esperadas por DW: ['ID_Cliente','CodigoClienteReal','NombreCliente']
    """
    clientes = df_dict.get('clientes', pd.DataFrame())
    if clientes.empty:
        logger.warning('dim_clientes: df clientes está vacío')
        return pd.DataFrame(columns=['ID_Cliente','CodigoClienteReal','NombreCliente'])

    df = clientes.copy()
    # Map simple columns
    df['CodigoClienteReal'] = df['ID_Cliente']

    result = df[['ID_Cliente','CodigoClienteReal','NombreCliente']].rename(columns={
        'NombreCliente':'NombreCliente'
    })
    logger.info(f"dim_clientes: preparado {len(result)} filas")
    return result
