import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def get_dependencies():
    # Dependencia opcional: tablas con fechas (hitos, tareas, asignaciones, pruebas, gastos)
    return []

def transform(df_dict: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_tiempo
    Nota: Esta transformación puede poblarse a partir de un rango de fechas o a partir de fechas encontradas en las tablas origen.
    Output columns sugeridas: ['ID_Tiempo','Fecha','ID_DiaSemana','ID_Mes','ID_Anio']
    """
    # Implementación mínima: devolver DataFrame vacío con columnas esperadas
    cols = ['ID_Tiempo','Fecha','ID_DiaSemana','ID_Mes','ID_Anio']
    logger.info('dim_tiempo: esqueleto creado')
    return pd.DataFrame(columns=cols)
