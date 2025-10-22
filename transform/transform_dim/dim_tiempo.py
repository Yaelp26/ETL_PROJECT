import pandas as pd
import logging
from typing import Dict
from datetime import datetime, timedelta
import sys
import os

# Agregar path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from transform.common import ensure_df, log_transform_info

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['proyectos', 'hitos', 'asignaciones', 'gastos']

def create_subdimensions():
    """Crear subdimensiones de tiempo"""
    # Días de la semana
    dias_semana = pd.DataFrame({
        'ID_Dia_Semana': list(range(1, 8)),
        'NumeroDia': list(range(1, 8))
    })
    
    # Meses
    meses = pd.DataFrame({
        'ID_Mes': list(range(1, 13)),
        'NumeroMes': list(range(1, 13))
    })
    
    # Años (rango básico)
    anios = pd.DataFrame({
        'ID_Anio': list(range(1, 6)),
        'NumeroAnio': [2020, 2021, 2022, 2023, 2024]
    })
    
    return dias_semana, meses, anios

def extract_dates_from_data(df_dict: Dict[str, pd.DataFrame]) -> list:
    fechas = set()
    
    # Proyectos
    proyectos = df_dict.get('proyectos', pd.DataFrame())
    if not proyectos.empty:
        for col in ['FechaInicio', 'FechaFin']:
            if col in proyectos.columns:
                fechas.update(proyectos[col].dropna().tolist())
    
    # Hitos
    hitos = df_dict.get('hitos', pd.DataFrame())
    if not hitos.empty:
        for col in ['FechaInicio', 'FechaFinPlanificada', 'FechaFinReal']:
            if col in hitos.columns:
                fechas.update(hitos[col].dropna().tolist())
    
    # Asignaciones
    asignaciones = df_dict.get('asignaciones', pd.DataFrame())
    if not asignaciones.empty and 'FechaAsignacion' in asignaciones.columns:
        fechas.update(asignaciones['FechaAsignacion'].dropna().tolist())
    
    # Filtrar fechas válidas
    fechas_validas = []
    for fecha in fechas:
        try:
            if isinstance(fecha, str):
                fecha_dt = pd.to_datetime(fecha)
            else:
                fecha_dt = fecha
            fechas_validas.append(fecha_dt.date())
        except:
            continue
    
    return sorted(list(set(fechas_validas)))

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    
    # Extraer fechas de los datos
    fechas_encontradas = extract_dates_from_data(df_dict)
    
    if not fechas_encontradas:
        logger.warning('dim_tiempo: No se encontraron fechas en los datos')
        # Crear rango básico por defecto
        start_date = datetime(2020, 1, 1).date()
        end_date = datetime(2024, 12, 31).date()
        fechas_rango = []
        current_date = start_date
        while current_date <= end_date:
            fechas_rango.append(current_date)
            current_date += timedelta(days=1)
        fechas_encontradas = fechas_rango[:100]  # Limitar para proyecto escolar
    
    # Crear dimensión tiempo
    dim_tiempo = []
    for i, fecha in enumerate(fechas_encontradas, 1):
        dim_tiempo.append({
            'ID_Tiempo': i,
            'Fecha': fecha,
            'ID_DiaSemana': fecha.weekday() + 1,  # 1=Lunes, 7=Domingo
            'ID_Mes': fecha.month,
            'ID_Anio': min(5, fecha.year - 2019)  # Mapear años a 1-5
        })
    
    result = pd.DataFrame(dim_tiempo)
    log_transform_info('dim_tiempo', len(fechas_encontradas), len(result))
    return result