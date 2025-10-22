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
    return ['proyectos', 'contratos', 'errores', 'asignaciones', 'hitos', 'tareas', 'dim_tiempo', 'dim_riesgos', 'dim_finanzas']

def calculate_project_metrics(proyecto_id: int, df_dict: Dict[str, pd.DataFrame]) -> Dict: 
    # Obtener datos principales
    proyectos = df_dict.get('proyectos', pd.DataFrame())
    contratos = df_dict.get('contratos', pd.DataFrame())
    dim_tiempo = df_dict.get('dim_tiempo', pd.DataFrame())
    dim_riesgos = df_dict.get('dim_riesgos', pd.DataFrame())
    dim_finanzas = df_dict.get('dim_finanzas', pd.DataFrame())
    
    proyecto_data = proyectos[proyectos['ID_Proyecto'] == proyecto_id]
    
    if proyecto_data.empty:
        return None
    
    proyecto = proyecto_data.iloc[0]
    
    # Inicializar métricas
    metrics = {
        'ID_Hecho': 0,  # Se asignará después
        'ID_Proyecto': proyecto_id,
        'ID_TiempoInicio': 0,
        'ID_TiempoFinalizacion': 0,
        'ID_Riesgo': 0,
        'ID_Finanza': 0,
        'DuracionRealDias': 0,
        'RetrasoDias': 0,  # Eliminada según especificaciones
        'PresupuestoCliente': 0.0,
        'CosteReal': 0.0,
        'DesviacionPresupuestal': 0.0,
        'PenalizacionesMonto': 0.0,
        'ProporcionCAPEX_OPEX': 0.0,
        'NumeroDefectosEncontrados': 0,
        'ProductividadPromedio': 0.0,
        'PorcentajeTareasRetrasadas': 0.0,
        'PorcentajeHitosRetrasados': 0.0
    }
    
    # === FECHAS Y DURACIÓN ===
    try:
        fecha_inicio = pd.to_datetime(proyecto['FechaInicio'])
        fecha_fin = pd.to_datetime(proyecto['FechaFin'])
        
        if pd.notna(fecha_inicio) and pd.notna(fecha_fin):
            metrics['DuracionRealDias'] = (fecha_fin - fecha_inicio).days
            
            # Mapear fechas a dim_tiempo
            if not dim_tiempo.empty:
                dim_tiempo['Fecha_date'] = pd.to_datetime(dim_tiempo['Fecha']).dt.date
                fecha_tiempo_map = dim_tiempo.set_index('Fecha_date')['ID_Tiempo'].to_dict()
                
                metrics['ID_TiempoInicio'] = fecha_tiempo_map.get(fecha_inicio.date(), 0)
                metrics['ID_TiempoFinalizacion'] = fecha_tiempo_map.get(fecha_fin.date(), 0)
    except:
        pass
    
    # === PRESUPUESTO Y COSTOS ===
    # PresupuestoCliente desde contratos
    if not contratos.empty and 'ID_Contrato' in proyecto:
        contrato_data = contratos[contratos['ID_Contrato'] == proyecto['ID_Contrato']]
        if not contrato_data.empty:
            metrics['PresupuestoCliente'] = float(contrato_data['ValorTotalContrato'].iloc[0] or 0)
    
    # CosteReal desde proyectos
    metrics['CosteReal'] = float(proyecto.get('costoReal', 0) or 0)
    
    # DesviacionPresupuestal
    metrics['DesviacionPresupuestal'] = metrics['CosteReal'] - metrics['PresupuestoCliente']
    
    # === FINANZAS (desde dim_finanzas para evitar consultas adicionales) ===
    if not dim_finanzas.empty:
        # Buscar el primer registro de finanzas para este proyecto
        finanzas_proyecto = dim_finanzas.head(1)  # Simplificado para proyecto escolar
        if not finanzas_proyecto.empty:
            metrics['ID_Finanza'] = finanzas_proyecto['ID_Finanza'].iloc[0]
            
            # PenalizacionesMonto desde dim_finanzas
            penalizaciones = dim_finanzas[dim_finanzas['TipoGasto'] == 'Penalizacion']
            metrics['PenalizacionesMonto'] = penalizaciones['Monto'].sum() if not penalizaciones.empty else 0.0
            
            # ProporcionCAPEX_OPEX desde dim_finanzas
            capex_total = dim_finanzas[dim_finanzas['Categoria'] == 'CapEx']['Monto'].sum()
            opex_total = dim_finanzas[dim_finanzas['Categoria'] == 'OpEx']['Monto'].sum()
            
            if opex_total > 0:
                metrics['ProporcionCAPEX_OPEX'] = float(capex_total / opex_total)
    
    # === RIESGOS ===
    if not dim_riesgos.empty:
        riesgo_proyecto = dim_riesgos.head(1)  # Simplificado
        if not riesgo_proyecto.empty:
            metrics['ID_Riesgo'] = riesgo_proyecto['ID_Riesgo'].iloc[0]
    
    # === DEFECTOS ===
    errores = df_dict.get('errores', pd.DataFrame())
    if not errores.empty:
        errores_proyecto = errores[errores['ID_Proyecto'] == proyecto_id]
        metrics['NumeroDefectosEncontrados'] = len(errores_proyecto)
    
    # === PRODUCTIVIDAD ===
    asignaciones = df_dict.get('asignaciones', pd.DataFrame())
    if not asignaciones.empty:
        asig_proyecto = asignaciones[asignaciones['ID_Proyecto'] == proyecto_id]
        if not asig_proyecto.empty:
            empleados_unicos = asig_proyecto['ID_Empleado'].nunique()
            if empleados_unicos > 0 and metrics['DuracionRealDias'] > 0:
                metrics['ProductividadPromedio'] = float(metrics['DuracionRealDias'] / empleados_unicos)
    
    # === PORCENTAJE TAREAS RETRASADAS ===
    tareas = df_dict.get('tareas', pd.DataFrame())
    if not tareas.empty:
        tareas_proyecto = tareas[tareas['ID_Proyecto'] == proyecto_id]
        if not tareas_proyecto.empty:
            total_tareas = len(tareas_proyecto)
            # Calcular RetrasoDias = DuracionReal - DuracionPlanificada
            tareas_con_retraso = 0
            for _, tarea in tareas_proyecto.iterrows():
                duracion_real = pd.to_numeric(tarea.get('DuracionReal', 0), errors='coerce') or 0
                duracion_plan = pd.to_numeric(tarea.get('DuracionPlanificada', 0), errors='coerce') or 0
                retraso_dias = duracion_real - duracion_plan
                if retraso_dias > 0:
                    tareas_con_retraso += 1
            
            metrics['PorcentajeTareasRetrasadas'] = (tareas_con_retraso / total_tareas) * 100 if total_tareas > 0 else 0.0
    
    # === PORCENTAJE HITOS RETRASADOS ===
    hitos = df_dict.get('hitos', pd.DataFrame())
    if not hitos.empty:
        hitos_proyecto = hitos[hitos['ID_Proyecto'] == proyecto_id]
        if not hitos_proyecto.empty:
            total_hitos = len(hitos_proyecto)
            # Calcular retraso de hitos similar a tareas
            hitos_con_retraso = 0
            for _, hito in hitos_proyecto.iterrows():
                fecha_fin_real = pd.to_datetime(hito.get('FechaFinReal'), errors='coerce')
                fecha_fin_plan = pd.to_datetime(hito.get('FechaFinPlanificada'), errors='coerce')
                
                if pd.notna(fecha_fin_real) and pd.notna(fecha_fin_plan):
                    retraso_dias = (fecha_fin_real - fecha_fin_plan).days
                    if retraso_dias > 0:
                        hitos_con_retraso += 1
            
            metrics['PorcentajeHitosRetrasados'] = (hitos_con_retraso / total_hitos) * 100 if total_hitos > 0 else 0.0
    
    return metrics

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    proyectos = ensure_df(df_dict.get('proyectos', pd.DataFrame()))
    
    if proyectos.empty:
        logger.warning('hechos_proyectos: No hay datos de proyectos')
        return pd.DataFrame(columns=[
            'ID_Hecho', 'ID_Proyecto', 'ID_TiempoInicio', 'ID_TiempoFinalizacion',
            'ID_Riesgo', 'ID_Finanza', 'DuracionRealDias', 'RetrasoDias',
            'PresupuestoCliente', 'CosteReal', 'DesviacionPresupuestal',
            'PenalizacionesMonto', 'ProporcionCAPEX_OPEX', 'NumeroDefectosEncontrados',
            'ProductividadPromedio', 'PorcentajeTareasRetrasadas', 'PorcentajeHitosRetrasados'
        ])
    
    # Calcular métricas para cada proyecto
    hechos_data = []
    hecho_id = 1
    
    for proyecto_id in proyectos['ID_Proyecto'].unique():
        metrics = calculate_project_metrics(proyecto_id, df_dict)
        if metrics:  # Solo agregar si se calcularon las métricas
            metrics['ID_Hecho'] = hecho_id
            hechos_data.append(metrics)
            hecho_id += 1
    
    if not hechos_data:
        logger.warning('hechos_proyectos: No se pudieron calcular métricas para ningún proyecto')
        return pd.DataFrame(columns=[
            'ID_Hecho', 'ID_Proyecto', 'ID_TiempoInicio', 'ID_TiempoFinalizacion',
            'ID_Riesgo', 'ID_Finanza', 'DuracionRealDias', 'RetrasoDias',
            'PresupuestoCliente', 'CosteReal', 'DesviacionPresupuestal',
            'PenalizacionesMonto', 'ProporcionCAPEX_OPEX', 'NumeroDefectosEncontrados',
            'ProductividadPromedio', 'PorcentajeTareasRetrasadas', 'PorcentajeHitosRetrasados'
        ])
    
    result = pd.DataFrame(hechos_data)
    
    # Reordenar columnas según especificaciones
    columnas_ordenadas = [
        'ID_Hecho', 'ID_Proyecto', 'ID_TiempoInicio', 'ID_TiempoFinalizacion',
        'ID_Riesgo', 'ID_Finanza', 'DuracionRealDias', 'RetrasoDias',
        'PresupuestoCliente', 'CosteReal', 'DesviacionPresupuestal',
        'PenalizacionesMonto', 'ProporcionCAPEX_OPEX', 'NumeroDefectosEncontrados',
        'ProductividadPromedio', 'PorcentajeTareasRetrasadas', 'PorcentajeHitosRetrasados'
    ]
    
    result = result[columnas_ordenadas]
    
    log_transform_info('hechos_proyectos', len(proyectos), len(result))
    return result