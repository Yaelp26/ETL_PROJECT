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
    """Tablas origen necesarias"""
    return ['clientes', 'contratos']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Transformación para dim_clientes
    
    Input: df_dict con:
        - 'clientes': DataFrame de clientes del SGP
        - 'contratos': DataFrame de contratos (para referencia)
    
    Output: DataFrame con esquema DW:
        - ID_Cliente: PK temporal
        - CodigoClienteReal: ID original del SGP  
        - NombreCliente: Nombre del cliente
    """
    # Obtener datos de entrada
    clientes = ensure_df(df_dict.get('clientes', pd.DataFrame()))
    
    if clientes.empty:
        logger.warning('dim_clientes: No hay datos de clientes para procesar')
        return pd.DataFrame(columns=['ID_Cliente', 'CodigoClienteReal', 'NombreCliente'])
    
    # Transformación simple
    df = clientes.copy()
    
    # Crear CodigoClienteReal (mapea al ID original del SGP)
    df['CodigoClienteReal'] = df['ID_Cliente']
    
    # Limpiar nombres básicamente
    df['NombreCliente'] = df['NombreCliente'].astype(str).str.strip()
    
    # Seleccionar columnas finales
    result = df[['ID_Cliente', 'CodigoClienteReal', 'NombreCliente']].copy()
    
    # Log del resultado
    log_transform_info('dim_clientes', len(clientes), len(result))
    
    return result

def test_transform():
    """Función de prueba simple"""
    # Datos de muestra
    sample_data = {
        'clientes': pd.DataFrame({
            'ID_Cliente': [1, 2, 3],
            'NombreCliente': ['Empresa A', 'Cliente B', 'Compañía C']
        }),
        'contratos': pd.DataFrame({
            'ID_Contrato': [1, 2],
            'ID_Cliente': [1, 2],
            'Estado': ['Terminado', 'Cancelado']
        })
    }
    
    result = transform(sample_data)
    print("Test dim_clientes:")
    print(result)
    return result

if __name__ == "__main__":
    test_transform()
