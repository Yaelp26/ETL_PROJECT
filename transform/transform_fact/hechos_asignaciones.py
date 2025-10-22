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
    return ['asignaciones', 'empleados', 'proyectos']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Obtener datos de entrada
    asignaciones = ensure_df(df_dict.get('asignaciones', pd.DataFrame()))
    empleados = ensure_df(df_dict.get('empleados', pd.DataFrame()))
    dim_tiempo = ensure_df(df_dict.get('dim_tiempo', pd.DataFrame()))
    
    if asignaciones.empty:
        logger.warning('hechos_asignaciones: No hay datos de asignaciones')
        return pd.DataFrame(columns=[
            'ID_HechoAsignacion', 'ID_Empleado', 'ID_Proyecto', 'ID_FechaAsignacion', 
            'HorasPlanificadas', 'HorasReales', 'ValorHoras'
        ])
    
    # Trabajar con copia
    df = asignaciones.copy()
    
    # ID_HechoAsignacion: ID generado secuencial
    df['ID_HechoAsignacion'] = range(1, len(df) + 1)
    
    # ID_FechaAsignacion: Mapear fecha a ID de dim_tiempo
    if not dim_tiempo.empty:
        # Convertir fechas a formato date para mapeo
        df['FechaAsignacion_date'] = pd.to_datetime(df['FechaAsignacion'], errors='coerce').dt.date
        dim_tiempo['Fecha_date'] = pd.to_datetime(dim_tiempo['Fecha'], errors='coerce').dt.date
        
        # Crear mapeo fecha -> ID_Tiempo
        fecha_tiempo_map = dim_tiempo.set_index('Fecha_date')['ID_Tiempo'].to_dict()
        df['ID_FechaAsignacion'] = df['FechaAsignacion_date'].map(fecha_tiempo_map)
        
        # Manejar fechas no encontradas
        fechas_no_encontradas = df['ID_FechaAsignacion'].isna().sum()
        if fechas_no_encontradas > 0:
            logger.warning(f'hechos_asignaciones: {fechas_no_encontradas} fechas no encontradas en dim_tiempo')
            df['ID_FechaAsignacion'] = df['ID_FechaAsignacion'].fillna(0).astype(int)
    else:
        logger.warning('hechos_asignaciones: dim_tiempo vacía, usando 0 como ID_FechaAsignacion')
        df['ID_FechaAsignacion'] = 0
    
    # HorasPlanificadas y HorasReales: Extraídas directamente del SGP
    df['HorasPlanificadas'] = pd.to_numeric(df['HorasPlanificadas'], errors='coerce').fillna(0)
    df['HorasReales'] = pd.to_numeric(df['HorasReales'], errors='coerce').fillna(0)
    
    # ValorHoras: CostoPorHora × HorasReales
    if not empleados.empty and 'CostoPorHora' in empleados.columns:
        # Crear mapeo empleado -> costo por hora
        emp_cost_map = empleados.set_index('ID_Empleado')['CostoPorHora'].to_dict()
        
        # Mapear costos y calcular valor
        df['CostoPorHora'] = df['ID_Empleado'].map(emp_cost_map).fillna(0)
        df['ValorHoras'] = df['HorasReales'] * df['CostoPorHora']
    else:
        df['ValorHoras'] = 0
        logger.warning('hechos_asignaciones: No se pudo calcular ValorHoras')
    
    # Seleccionar columnas finales
    result = df[[
        'ID_HechoAsignacion', 'ID_Empleado', 'ID_Proyecto', 'ID_FechaAsignacion',
        'HorasPlanificadas', 'HorasReales', 'ValorHoras'
    ]].copy()
    
    # Log del resultado
    log_transform_info('hechos_asignaciones', len(asignaciones), len(result))
    
    return result