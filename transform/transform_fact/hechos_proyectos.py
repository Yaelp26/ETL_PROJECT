"""
Transformación hechos_proyectos - Proyecto Escolar ETL
Tabla de hechos principal con métricas agregadas por proyecto
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
    return ['proyectos', 'hitos', 'tareas', 'gastos', 'penalizaciones', 'riesgos', 'asignaciones']

def calculate_project_metrics(proyecto_id: int, df_dict: Dict[str, pd.DataFrame]) -> Dict:
    """Calcula métricas específicas para un proyecto"""
    metrics = {
        'ID_Proyecto': proyecto_id,
        'DuracionRealDias': 0,
        'RetrasoDias': 0,
        'PresupuestoCliente': 0.0,
        'CosteReal': 0.0,
        'DesviacionPresupuestal': 0.0,
        'PenalizacionesMonto': 0.0,
        'NumeroDefectosEncontrados': 0,
        'ProductividadPromedio': 0.0,
        'PorcentajeTareasRetrasadas': 0.0,
        'PorcentajeHitosRetrasados': 0.0
    }
    
    # Obtener datos del proyecto
    proyectos = df_dict.get('proyectos', pd.DataFrame())
    proyecto_data = proyectos[proyectos['ID_Proyecto'] == proyecto_id]
    
    if not proyecto_data.empty:
        # Presupuesto cliente
        metrics['PresupuestoCliente'] = float(proyecto_data['ValorTotalContrato'].iloc[0] or 0)
        
        # Duración del proyecto
        if 'FechaInicio' in proyecto_data.columns and 'FechaFin' in proyecto_data.columns:
            try:
                inicio = pd.to_datetime(proyecto_data['FechaInicio'].iloc[0])
                fin = pd.to_datetime(proyecto_data['FechaFin'].iloc[0])
                if pd.notna(inicio) and pd.notna(fin):
                    metrics['DuracionRealDias'] = (fin - inicio).days
            except:
                pass
    
    # Métricas de hitos
    hitos = df_dict.get('hitos', pd.DataFrame())
    hitos_proyecto = hitos[hitos['ID_Proyecto'] == proyecto_id] if not hitos.empty else pd.DataFrame()
    
    if not hitos_proyecto.empty:
        total_hitos = len(hitos_proyecto)
        hitos_retrasados = (hitos_proyecto.get('dias_retraso', 0) > 0).sum()
        metrics['PorcentajeHitosRetrasados'] = (hitos_retrasados / total_hitos * 100) if total_hitos > 0 else 0
        
        # Retraso promedio
        retrasos = pd.to_numeric(hitos_proyecto.get('dias_retraso', 0), errors='coerce').fillna(0)
        metrics['RetrasoDias'] = int(retrasos.mean()) if len(retrasos) > 0 else 0
    
    # Métricas de tareas
    tareas = df_dict.get('tareas', pd.DataFrame())
    tareas_proyecto = tareas[tareas['ID_Proyecto'] == proyecto_id] if not tareas.empty else pd.DataFrame()
    
    if not tareas_proyecto.empty:
        total_tareas = len(tareas_proyecto)
        tareas_retrasadas = (pd.to_numeric(tareas_proyecto.get('desviacion_duracion', 0), errors='coerce') > 0).sum()
        metrics['PorcentajeTareasRetrasadas'] = (tareas_retrasadas / total_tareas * 100) if total_tareas > 0 else 0
    
    # Costos reales
    gastos = df_dict.get('gastos', pd.DataFrame())
    gastos_proyecto = gastos[gastos['ID_Proyecto'] == proyecto_id] if not gastos.empty else pd.DataFrame()
    
    if not gastos_proyecto.empty:
        metrics['CosteReal'] = float(pd.to_numeric(gastos_proyecto['Monto'], errors='coerce').sum())
    
    # Asignaciones y productividad
    asignaciones = df_dict.get('asignaciones', pd.DataFrame())
    asig_proyecto = asignaciones[asignaciones['ID_Proyecto'] == proyecto_id] if not asignaciones.empty else pd.DataFrame()
    
    if not asig_proyecto.empty:
        empleados_unicos = asig_proyecto['ID_Empleado'].nunique()
        horas_totales = pd.to_numeric(asig_proyecto['HorasReales'], errors='coerce').sum()
        
        if empleados_unicos > 0 and metrics['DuracionRealDias'] > 0:
            metrics['ProductividadPromedio'] = float(metrics['DuracionRealDias'] / empleados_unicos)
        
        # Sumar al costo real si hay valor de horas
        if 'ValorHoras' in asig_proyecto.columns:
            costo_horas = pd.to_numeric(asig_proyecto['ValorHoras'], errors='coerce').sum()
            metrics['CosteReal'] += float(costo_horas or 0)
    
    # Penalizaciones
    penalizaciones = df_dict.get('penalizaciones', pd.DataFrame())
    if not penalizaciones.empty and 'ID_Contrato' in proyecto_data.columns:
        contrato_id = proyecto_data['ID_Contrato'].iloc[0]
        penal_contrato = penalizaciones[penalizaciones['ID_Contrato'] == contrato_id]
        if not penal_contrato.empty:
            metrics['PenalizacionesMonto'] = float(pd.to_numeric(penal_contrato['Monto'], errors='coerce').sum())
    
    # Desviación presupuestal
    metrics['DesviacionPresupuestal'] = metrics['CosteReal'] - metrics['PresupuestoCliente']
    
    # Defectos (simplificado - contar errores)
    errores = df_dict.get('errores', pd.DataFrame())
    if not errores.empty:
        errores_proyecto = errores[errores['ID_Proyecto'] == proyecto_id]
        metrics['NumeroDefectosEncontrados'] = len(errores_proyecto)
    
    return metrics

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Transformación para hechos_proyectos"""
    proyectos = ensure_df(df_dict.get('proyectos', pd.DataFrame()))
    
    if proyectos.empty:
        logger.warning('hechos_proyectos: No hay datos de proyectos')
        return pd.DataFrame(columns=[
            'ID_Proyecto', 'DuracionRealDias', 'RetrasoDias', 'PresupuestoCliente',
            'CosteReal', 'DesviacionPresupuestal', 'PenalizacionesMonto',
            'NumeroDefectosEncontrados', 'ProductividadPromedio',
            'PorcentajeTareasRetrasadas', 'PorcentajeHitosRetrasados'
        ])
    
    # Calcular métricas para cada proyecto
    hechos_data = []
    for proyecto_id in proyectos['ID_Proyecto'].unique():
        metrics = calculate_project_metrics(proyecto_id, df_dict)
        hechos_data.append(metrics)
    
    result = pd.DataFrame(hechos_data)
    log_transform_info('hechos_proyectos', len(proyectos), len(result))
    return result

def test_transform():
    sample_data = {
        'proyectos': pd.DataFrame({
            'ID_Proyecto': [1, 2],
            'ID_Contrato': [1, 2],
            'ValorTotalContrato': [100000.0, 75000.0],
            'FechaInicio': ['2023-01-01', '2023-02-01'],
            'FechaFin': ['2023-06-01', '2023-07-01']
        }),
        'gastos': pd.DataFrame({
            'ID_Proyecto': [1, 1, 2],
            'Monto': [15000.0, 8000.0, 12000.0]
        }),
        'hitos': pd.DataFrame({
            'ID_Proyecto': [1, 1, 2],
            'dias_retraso': [5, 0, -2]
        })
    }
    result = transform(sample_data)
    print("Test hechos_proyectos:")
    print(result)
    return result

if __name__ == "__main__":
    test_transform()
