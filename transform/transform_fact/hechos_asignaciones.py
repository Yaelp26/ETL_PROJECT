"""
Transformación hechos_asignaciones - Proyecto Escolar ETL
Tabla de hechos con métricas de asignaciones de empleados a proyectos
"""
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
    return ['asignaciones', 'empleados', 'proyectos']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Transformación para hechos_asignaciones
    
    Input: df_dict con:
        - 'asignaciones': Asignaciones empleado-proyecto
        - 'empleados': Datos de empleados (para CostoPorHora)
        - 'proyectos': Datos de proyectos (para validación)
    
    Output: DataFrame con esquema DW:
        - ID_Empleado: FK a dim_empleados
        - ID_Proyecto: FK a dim_proyectos  
        - ID_FechaAsignacion: FK a dim_tiempo (fecha como string por simplicidad)
        - HorasPlanificadas: Horas estimadas
        - HorasReales: Horas trabajadas realmente
        - ValorHoras: Costo monetario (HorasReales × CostoPorHora)
    """
    # Obtener datos de entrada
    asignaciones = ensure_df(df_dict.get('asignaciones', pd.DataFrame()))
    empleados = ensure_df(df_dict.get('empleados', pd.DataFrame()))
    
    if asignaciones.empty:
        logger.warning('hechos_asignaciones: No hay datos de asignaciones')
        return pd.DataFrame(columns=[
            'ID_Empleado', 'ID_Proyecto', 'ID_FechaAsignacion', 
            'HorasPlanificadas', 'HorasReales', 'ValorHoras'
        ])
    
    # Trabajar con copia
    df = asignaciones.copy()
    
    # Calcular ValorHoras si tenemos datos de empleados
    if not empleados.empty and 'CostoPorHora' in empleados.columns:
        # Crear mapeo empleado -> costo por hora
        emp_cost_map = empleados.set_index('ID_Empleado')['CostoPorHora'].to_dict()
        
        # Mapear costos y calcular valor
        df['CostoPorHora'] = df['ID_Empleado'].map(emp_cost_map).fillna(0)
        df['HorasReales_num'] = pd.to_numeric(df['HorasReales'], errors='coerce').fillna(0)
        df['ValorHoras'] = df['HorasReales_num'] * df['CostoPorHora']
    else:
        df['ValorHoras'] = 0
        logger.warning('hechos_asignaciones: No se pudo calcular ValorHoras')
    
    # Preparar fecha para ID_FechaAsignacion (simplificado)
    df['ID_FechaAsignacion'] = df['FechaAsignacion'].astype(str)
    
    # Asegurar tipos numéricos
    df['HorasPlanificadas'] = pd.to_numeric(df['HorasPlanificadas'], errors='coerce').fillna(0)
    df['HorasReales'] = pd.to_numeric(df['HorasReales'], errors='coerce').fillna(0)
    
    # Seleccionar columnas finales
    result = df[[
        'ID_Empleado', 'ID_Proyecto', 'ID_FechaAsignacion',
        'HorasPlanificadas', 'HorasReales', 'ValorHoras'
    ]].copy()
    
    # Log del resultado
    log_transform_info('hechos_asignaciones', len(asignaciones), len(result))
    
    return result

def test_transform():
    """Función de prueba simple"""
    # Datos de muestra
    sample_data = {
        'asignaciones': pd.DataFrame({
            'ID_Asignacion': [1, 2, 3],
            'ID_Empleado': [1, 2, 1],
            'ID_Proyecto': [1, 1, 2],
            'HorasPlanificadas': [40, 30, 50],
            'HorasReales': [45, 28, 55],
            'FechaAsignacion': ['2024-01-15', '2024-01-20', '2024-02-01']
        }),
        'empleados': pd.DataFrame({
            'ID_Empleado': [1, 2],
            'NombreCompleto': ['Juan Pérez', 'Ana García'],
            'CostoPorHora': [25.0, 30.0]
        }),
        'proyectos': pd.DataFrame({
            'ID_Proyecto': [1, 2],
            'NombreProyecto': ['Proyecto A', 'Proyecto B']
        })
    }
    
    result = transform(sample_data)
    print("Test hechos_asignaciones:")
    print(result)
    print(f"ValorHoras calculado: {result['ValorHoras'].sum()}")
    return result

if __name__ == "__main__":
    test_transform()
