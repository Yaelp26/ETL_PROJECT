import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['asignaciones','empleados','proyectos','dim_tiempo']

def transform(df_dict: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    """Transformación para hechos_asignaciones
    Output sugerido: ['ID_Empleado','ID_Proyecto','ID_FechaAsignacion','HorasPlanificadas','HorasReales','ValorHoras']
    """
    asignaciones = df_dict.get('asignaciones', pd.DataFrame())
    empleados = df_dict.get('empleados', pd.DataFrame())

    if asignaciones.empty:
        logger.warning('hechos_asignaciones: df asignaciones vacío')
        return pd.DataFrame(columns=['ID_Empleado','ID_Proyecto','ID_FechaAsignacion','HorasPlanificadas','HorasReales','ValorHoras'])

    df = asignaciones.copy()
    # Si existe CostoPorHora en empleados, calcular ValorHoras
    if 'CostoPorHora' in empleados.columns:
        emp_cost = empleados.set_index('ID_Empleado')['CostoPorHora']
        df['CostoPorHora'] = df['ID_Empleado'].map(emp_cost)
        df['ValorHoras'] = df['HorasReales'].astype(float).fillna(0) * df['CostoPorHora'].astype(float).fillna(0)
    else:
        df['ValorHoras'] = None

    result = df[['ID_Empleado','ID_Proyecto','FechaAsignacion','HorasPlanificadas','HorasReales','ValorHoras']]
    result = result.rename(columns={'FechaAsignacion':'ID_FechaAsignacion'})
    logger.info(f"hechos_asignaciones: preparado {len(result)} filas")
    return result
