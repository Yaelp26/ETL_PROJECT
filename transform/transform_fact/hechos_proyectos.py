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
    return ['proyectos', 'contratos', 'errores', 'asignaciones', 'hitos', 'tareas', 'gastos', 'penalizaciones', 'riesgos', 'dim_tiempo', 'dim_proyectos']

def calculate_project_metrics(proyecto_id: int, df_dict: Dict[str, pd.DataFrame]) -> Dict: 
    # Obtener datos principales
    proyectos = df_dict.get('proyectos', pd.DataFrame())
    contratos = df_dict.get('contratos', pd.DataFrame())
    gastos = df_dict.get('gastos', pd.DataFrame())
    penalizaciones = df_dict.get('penalizaciones', pd.DataFrame())
    riesgos = df_dict.get('riesgos', pd.DataFrame())
    dim_tiempo = df_dict.get('dim_tiempo', pd.DataFrame())
    
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
            
            # Calcular RetrasoDias basado en duración planificada de hitos
            hitos_proyecto = df_dict.get('hitos', pd.DataFrame())
            if not hitos_proyecto.empty:
                hitos_del_proyecto = hitos_proyecto[hitos_proyecto['ID_Proyecto'] == proyecto_id]
                if not hitos_del_proyecto.empty:
                    # Usar el rango de fechas planificadas de hitos como referencia
                    fechas_planificadas = pd.to_datetime(hitos_del_proyecto['FechaFinPlanificada'], errors='coerce')
                    fechas_reales = pd.to_datetime(hitos_del_proyecto['FechaFinReal'], errors='coerce')
                    
                    if fechas_planificadas.notna().any() and fechas_reales.notna().any():
                        # Calcular la fecha fin planificada máxima de hitos
                        fecha_fin_planificada_proyecto = fechas_planificadas.max()
                        if pd.notna(fecha_fin_planificada_proyecto):
                            duracion_planificada = (fecha_fin_planificada_proyecto - fecha_inicio).days
                            metrics['RetrasoDias'] = max(0, metrics['DuracionRealDias'] - duracion_planificada)
            
            # Mapear fechas a dim_tiempo
            if not dim_tiempo.empty:
                dim_tiempo['Fecha_date'] = pd.to_datetime(dim_tiempo['Fecha']).dt.date
                fecha_tiempo_map = dim_tiempo.set_index('Fecha_date')['ID_Tiempo'].to_dict()
                
                metrics['ID_TiempoInicio'] = fecha_tiempo_map.get(fecha_inicio.date(), 0)
                metrics['ID_TiempoFinalizacion'] = fecha_tiempo_map.get(fecha_fin.date(), 0)
    except:
        pass
    
    # === PRESUPUESTO Y COSTOS ===
    # PresupuestoCliente desde el proyecto directamente (ValorTotalContrato está duplicado aquí)
    metrics['PresupuestoCliente'] = float(proyecto.get('ValorTotalContrato', 0) or 0)
    
    # CosteReal: CALCULAR sumando todos los gastos del proyecto
    # (El generador de datos dejó todos los costos reales en 0, así que calculamos desde gastos)
    gastos_proyecto = gastos[gastos['ID_Proyecto'] == proyecto_id] if not gastos.empty else pd.DataFrame()
    if not gastos_proyecto.empty:
        metrics['CosteReal'] = float(gastos_proyecto['Monto'].sum())
    else:
        metrics['CosteReal'] = 0.0
    
    # DesviacionPresupuestal: CORREGIDO - Presupuesto menos Costo Real
    # (positivo = ahorro, negativo = sobrecosto)
    metrics['DesviacionPresupuestal'] = metrics['PresupuestoCliente'] - metrics['CosteReal']
    
    # === FINANZAS (usar tablas originales gastos y penalizaciones) ===
    # ID_Finanza: usar el primer gasto del proyecto como referencia
    gastos_proyecto = gastos[gastos['ID_Proyecto'] == proyecto_id] if not gastos.empty else pd.DataFrame()
    if not gastos_proyecto.empty:
        metrics['ID_Finanza'] = gastos_proyecto['ID_Gasto'].iloc[0]  # Primera transacción como referencia
        
        # PenalizacionesMonto: desde gastos del proyecto (case insensitive)
        penalizaciones_gastos = gastos_proyecto[gastos_proyecto['TipoGasto'].str.lower().str.contains('penalizacion', na=False)]
        monto_penalizaciones_gastos = penalizaciones_gastos['Monto'].sum() if not penalizaciones_gastos.empty else 0.0
        
        # También agregar penalizaciones directas por contrato del proyecto
        proyecto_data = proyectos[proyectos['ID_Proyecto'] == proyecto_id]
        if not proyecto_data.empty and not penalizaciones.empty:
            id_contrato = proyecto_data['ID_Contrato'].iloc[0]
            penalizaciones_contrato = penalizaciones[penalizaciones['ID_Contrato'] == id_contrato]
            monto_penalizaciones_contrato = penalizaciones_contrato['Monto'].sum() if not penalizaciones_contrato.empty else 0.0
        else:
            monto_penalizaciones_contrato = 0.0
            
        metrics['PenalizacionesMonto'] = monto_penalizaciones_gastos + monto_penalizaciones_contrato
        
        # ProporcionCAPEX_OPEX desde gastos del proyecto
        capex_total = gastos_proyecto[gastos_proyecto['Categoria'].str.upper() == 'CAPEX']['Monto'].sum()
        opex_total = gastos_proyecto[gastos_proyecto['Categoria'].str.upper() == 'OPEX']['Monto'].sum()
        
        if opex_total > 0:
            metrics['ProporcionCAPEX_OPEX'] = float(capex_total / opex_total)
        elif capex_total > 0:
            metrics['ProporcionCAPEX_OPEX'] = float('inf')  # Solo CAPEX
        else:
            metrics['ProporcionCAPEX_OPEX'] = 0.0
    else:
        # Si no hay gastos, buscar penalizaciones por contrato
        proyecto_data = proyectos[proyectos['ID_Proyecto'] == proyecto_id]
        if not proyecto_data.empty and not penalizaciones.empty:
            id_contrato = proyecto_data['ID_Contrato'].iloc[0]
            penalizaciones_contrato = penalizaciones[penalizaciones['ID_Contrato'] == id_contrato]
            metrics['PenalizacionesMonto'] = penalizaciones_contrato['Monto'].sum() if not penalizaciones_contrato.empty else 0.0
    
    # === RIESGOS (usar tabla original riesgos) ===
    if not riesgos.empty:
        riesgo_proyecto = riesgos[riesgos['ID_Proyecto'] == proyecto_id]
        if not riesgo_proyecto.empty:
            metrics['ID_Riesgo'] = riesgo_proyecto['ID_Riesgo'].iloc[0]
        else:
            metrics['ID_Riesgo'] = 1  # Default si no hay riesgos específicos
    
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
    # Usar dim_proyectos para garantizar integridad referencial
    dim_proyectos = ensure_df(df_dict.get('dim_proyectos', pd.DataFrame()))
    proyectos = ensure_df(df_dict.get('proyectos', pd.DataFrame()))  # Para datos adicionales
    
    if dim_proyectos.empty:
        logger.warning('hechos_proyectos: No hay datos de dim_proyectos')
        return pd.DataFrame(columns=[
            'ID_Hecho', 'ID_Proyecto', 'ID_TiempoInicio', 'ID_TiempoFinalizacion',
            'ID_Riesgo', 'ID_Finanza', 'DuracionRealDias', 'RetrasoDias',
            'PresupuestoCliente', 'CosteReal', 'DesviacionPresupuestal',
            'PenalizacionesMonto', 'ProporcionCAPEX_OPEX', 'NumeroDefectosEncontrados',
            'ProductividadPromedio', 'PorcentajeTareasRetrasadas', 'PorcentajeHitosRetrasados'
        ])
    
    # Calcular métricas SOLO para proyectos válidos de dim_proyectos
    hechos_data = []
    hecho_id = 1
    
    logger.info(f'hechos_proyectos: Procesando {len(dim_proyectos)} proyectos válidos de dim_proyectos')
    
    for proyecto_id in dim_proyectos['ID_Proyecto'].unique():
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