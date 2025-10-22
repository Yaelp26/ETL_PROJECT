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
    return ['proyectos', 'contratos', 'clientes']  # Agregar clientes para validar FK

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    proyectos = ensure_df(df_dict.get('proyectos', pd.DataFrame()))
    clientes = ensure_df(df_dict.get('clientes', pd.DataFrame()))
    
    if proyectos.empty:
        logger.warning('dim_proyectos: No hay datos de proyectos')
        return pd.DataFrame(columns=['ID_Proyecto','CodigoProyecto','Version','Cancelado','ID_Cliente'])

    df = proyectos.copy()
    df['CodigoProyecto'] = df['ID_Proyecto']
    
    # Determinar si está cancelado
    if 'EstadoProyecto' in df.columns:
        df['Cancelado'] = df['EstadoProyecto'].apply(lambda x: 1 if str(x).upper() == 'CANCELADO' else 0)
    else:
        df['Cancelado'] = 0
    
    # Limpiar versión
    df['Version'] = df['Version'].fillna('1.0').astype(str).str.strip()
    
    # Validar que ID_Cliente existe en dim_clientes (opcional para proyecto escolar)
    if not clientes.empty:
        clientes_validos = set(clientes['ID_Cliente'].unique())
        df = df[df['ID_Cliente'].isin(clientes_validos)]
        if len(df) < len(proyectos):
            logger.info(f'dim_proyectos: Filtrados {len(proyectos) - len(df)} proyectos con clientes inexistentes')

    result = df[['ID_Proyecto','CodigoProyecto','Version','Cancelado','ID_Cliente']].copy()
    log_transform_info('dim_proyectos', len(proyectos), len(result))
    return result
