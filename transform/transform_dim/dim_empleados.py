import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['empleados','asignaciones']

def transform(df_dict: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_empleados
    Output columns (sugeridas): ['ID_Empleado','CodigoEmpleado','NombreCompleto','Rol','Seniority']
    """
    empleados = df_dict.get('empleados', pd.DataFrame())
    if empleados.empty:
        logger.warning('dim_empleados: df empleados vacío')
        return pd.DataFrame(columns=['ID_Empleado','CodigoEmpleado','NombreCompleto','Rol','Seniority'])

    df = empleados.copy()
    df['CodigoEmpleado'] = df['ID_Empleado']

    result = df[['ID_Empleado','CodigoEmpleado','NombreCompleto','Rol','Seniority']]
    logger.info(f"dim_empleados: preparado {len(result)} filas")
    return result
