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
    return ['hitos', 'proyectos']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Transformación para dim_hitos
    
    Input: df_dict con:
        - 'hitos': DataFrame de hitos del SGP con FechaInicio, FechaFinPlanificada, FechaFinReal
        - 'proyectos': DataFrame de proyectos (para validación)
    
    Output: DataFrame con esquema DW:
        - ID_Hito: PK temporal para el DW  
        - CodigoHito: ID original del SGP
        - ID_Proyecto: FK hacia dim_proyectos
        - ID_FechaInicio: FK hacia dim_tiempo (basado en FechaInicio)
        - ID_FechaFinalizacion: FK hacia dim_tiempo (basado en FechaFinReal)
        - Retraso_days: Diferencia entre FechaFinReal - FechaFinPlanificada
    """
    hitos = ensure_df(df_dict.get('hitos', pd.DataFrame()))
    
    if hitos.empty:
        logger.warning('dim_hitos: No hay datos de hitos')
        return pd.DataFrame(columns=['ID_Hito','CodigoHito','ID_Proyecto','ID_FechaInicio','ID_FechaFinalizacion','Retraso_days'])

    df = hitos.copy()
    df['CodigoHito'] = df['ID_Hito']
    
    # Función para convertir fecha a ID de dim_tiempo
    def fecha_to_tiempo_id(fecha_str):
        """
        Convierte fecha a ID de dim_tiempo
        dim_tiempo tiene estructura: ID_Tiempo, Fecha, ID_DiaSemana, ID_Mes, ID_Anio
        """
        if pd.isna(fecha_str) or str(fecha_str) == 'None':
            return None
        try:
            fecha = pd.to_datetime(fecha_str)
            # Generar ID basado en días desde 2020-01-01 (fecha base de dim_tiempo)
            base_date = pd.to_datetime('2020-01-01')
            days_diff = (fecha - base_date).days + 1
            return max(1, days_diff) if days_diff > 0 else 1
        except:
            return 1  # Default al primer ID de tiempo
    
    # Mapear fechas a IDs de dim_tiempo
    df['ID_FechaInicio'] = df['FechaInicio'].apply(fecha_to_tiempo_id)
    df['ID_FechaFinalizacion'] = df['FechaFinReal'].apply(fecha_to_tiempo_id)
    
    # Calcular retraso: FechaFinReal - FechaFinPlanificada
    def calcular_retraso(fecha_real, fecha_planificada):
        """Calcula días de retraso entre fecha real y planificada"""
        try:
            if pd.isna(fecha_real) or pd.isna(fecha_planificada):
                return 0
            real = pd.to_datetime(fecha_real)
            planificada = pd.to_datetime(fecha_planificada)
            return (real - planificada).days
        except:
            return 0
    
    df['Retraso_days'] = df.apply(
        lambda row: calcular_retraso(row['FechaFinReal'], row['FechaFinPlanificada']), 
        axis=1
    )

    # Seleccionar columnas finales para el DW
    result = df[['ID_Hito','CodigoHito','ID_Proyecto','ID_FechaInicio','ID_FechaFinalizacion','Retraso_days']].copy()
    
    log_transform_info('dim_hitos', len(hitos), len(result))
    return result
