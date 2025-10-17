#!/usr/bin/env python3
"""
Corrección del cálculo de ganancias en la tabla de hechos
"""

import sys
import os
sys.path.append('scripts')

from src.db_connector import get_db_connection

def fix_ganancias():
    """Corrige el cálculo de ganancias en la tabla de hechos."""
    print("🔧 CORRIGIENDO CÁLCULO DE GANANCIAS")
    print("=" * 50)
    
    source_conn = get_db_connection('origen')
    dest_conn = get_db_connection('destino')
    
    if not source_conn or not dest_conn:
        print("❌ Error: No se pudieron establecer las conexiones")
        return False
    
    try:
        source_cursor = source_conn.cursor()
        dest_cursor = dest_conn.cursor()
        
        # 1. Limpiar tabla de hechos actual
        print("\n🗑️ Limpiando tabla de hechos actual...")
        dest_cursor.execute("DELETE FROM hechos_proyectos")
        dest_conn.commit()
        print("   ✅ Tabla de hechos limpiada")
        
        # 2. Obtener mapeos del DW
        dest_cursor.execute("SELECT ID_proyectos, CodigoProyecto FROM dim_proyectos")
        mapeo_proyectos = {int(row[1]): row[0] for row in dest_cursor.fetchall()}
        
        dest_cursor.execute("SELECT ID_Tiempo, Fecha FROM dim_tiempo")
        mapeo_fechas = {row[1]: row[0] for row in dest_cursor.fetchall()}
        
        print(f"   📋 Mapeos: {len(mapeo_proyectos)} proyectos, {len(mapeo_fechas)} fechas")
        
        # 3. Query CORREGIDA con ValorTotalContrato
        query_corregida = """
        SELECT 
            p.ID_Proyecto,
            p.FechaInicio,
            p.FechaFin,
            p.CostoPresupuestado,
            p.CostoReal,
            c.ValorTotalContrato,  -- ¡AQUÍ ESTÁ EL VALOR!
            
            -- Agregación de horas por proyecto
            COALESCE((
                SELECT SUM(a.HorasEstimadas) 
                FROM asignaciones a 
                INNER JOIN tareas t ON a.tareas_ID_Tarea = t.ID_Tarea 
                WHERE t.ID_Proyecto = p.ID_Proyecto
            ), 0) as HorasPlanificadas,
            
            COALESCE((
                SELECT SUM(a.HorasReales) 
                FROM asignaciones a 
                INNER JOIN tareas t ON a.tareas_ID_Tarea = t.ID_Tarea 
                WHERE t.ID_Proyecto = p.ID_Proyecto
            ), 0) as HorasReales,
            
            -- Contar errores por proyecto
            COALESCE((
                SELECT COUNT(e.ID_Error)
                FROM errores e
                INNER JOIN tareas t ON e.ID_Tarea = t.ID_Tarea
                WHERE t.ID_Proyecto = p.ID_Proyecto
            ), 0) as NumeroDefectosEncontrados,
            
            -- Contar pruebas exitosas por proyecto
            COALESCE((
                SELECT COUNT(pr.ID_Pruebas)
                FROM pruebas pr
                INNER JOIN tareas t ON pr.tareas_ID_Tarea = t.ID_Tarea
                WHERE t.ID_Proyecto = p.ID_Proyecto AND pr.Exitoso = 1
            ), 0) as NumeroTestExitosos
            
        FROM proyectos p
        INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
        WHERE p.Estado IN ('Cancelado', 'Cerrado')
        AND c.Estado IN ('Cerrado', 'Cancelado')
        ORDER BY p.ID_Proyecto
        """
        
        print("\n📊 Extrayendo datos con ValorTotalContrato...")
        source_cursor.execute(query_corregida)
        hechos_data = source_cursor.fetchall()
        source_cursor.close()
        
        print(f"   ✅ {len(hechos_data)} registros extraídos")
        
        # 4. Procesar e insertar con cálculo correcto de ganancias
        print("\n💰 Calculando ganancias correctamente...")
        hechos_insertados = 0
        ganancias_positivas = 0
        ganancias_negativas = 0
        
        for hecho in hechos_data:
            proyecto_id = hecho[0]
            fecha_inicio = hecho[1]
            fecha_fin = hecho[2]
            costo_presupuestado = hecho[3]
            costo_real = hecho[4]
            valor_contrato = hecho[5]  # ¡AQUÍ USAMOS EL VALOR CORRECTO!
            horas_planificadas = hecho[6]
            horas_reales = hecho[7]
            defectos = hecho[8]
            tests_exitosos = hecho[9]
            
            # CÁLCULO CORRECTO DE GANANCIAS
            if valor_contrato is not None and costo_real is not None:
                ganancias = float(valor_contrato) - float(costo_real)
                if ganancias > 0:
                    ganancias_positivas += 1
                else:
                    ganancias_negativas += 1
            else:
                ganancias = 0.0
            
            # Obtener IDs del DW
            proyecto_dw_id = mapeo_proyectos.get(proyecto_id)
            fecha_inicio_id = mapeo_fechas.get(fecha_inicio) if fecha_inicio else None
            fecha_fin_id = mapeo_fechas.get(fecha_fin) if fecha_fin else None
            
            if proyecto_dw_id:
                dest_cursor.execute(
                    """INSERT INTO hechos_proyectos 
                       (ID_proyecto, ID_TiempoInicio, ID_TiempoFin, HorasPlanificadas, HorasReales, 
                        CostoPresupuestado, CostoReal, NumeroTestExitosos, NumeroDefectosEncontrados, Ganancias) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (proyecto_dw_id, fecha_inicio_id, fecha_fin_id, horas_planificadas, horas_reales,
                     costo_presupuestado, costo_real, tests_exitosos, defectos, ganancias)
                )
                hechos_insertados += 1
        
        dest_cursor.close()
        dest_conn.commit()
        
        print(f"   ✅ {hechos_insertados} registros insertados")
        print(f"   📈 Proyectos con ganancias positivas: {ganancias_positivas}")
        print(f"   📉 Proyectos con ganancias negativas: {ganancias_negativas}")
        
        # 5. Verificar resultados
        print(f"\n🔍 VERIFICACIÓN DE GANANCIAS:")
        dest_cursor = dest_conn.cursor()
        
        dest_cursor.execute("""
            SELECT h.ID_proyecto, h.CostoReal, h.Ganancias, 
                   CASE WHEN h.Ganancias > 0 THEN 'GANANCIA' ELSE 'PÉRDIDA' END as Resultado
            FROM hechos_proyectos h 
            WHERE h.Ganancias != 0
            ORDER BY h.Ganancias DESC
            LIMIT 10
        """)
        
        muestra_ganancias = dest_cursor.fetchall()
        for i, resultado in enumerate(muestra_ganancias, 1):
            print(f"   {i}. Proyecto {resultado[0]}: Costo Real: ${resultado[1]:,.2f}, Ganancia: ${resultado[2]:,.2f} ({resultado[3]})")
        
        # Estadísticas finales
        dest_cursor.execute("SELECT AVG(Ganancias), MIN(Ganancias), MAX(Ganancias) FROM hechos_proyectos WHERE Ganancias != 0")
        stats = dest_cursor.fetchone()
        if stats[0]:
            print(f"\n📊 ESTADÍSTICAS DE GANANCIAS:")
            print(f"   💰 Ganancia promedio: ${stats[0]:,.2f}")
            print(f"   📉 Menor ganancia: ${stats[1]:,.2f}")
            print(f"   📈 Mayor ganancia: ${stats[2]:,.2f}")
        
        dest_cursor.close()
        
        print(f"\n🎉 ¡Ganancias corregidas exitosamente!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        dest_conn.rollback()
        return False
        
    finally:
        if source_conn and source_conn.is_connected():
            source_conn.close()
        if dest_conn and dest_conn.is_connected():
            dest_conn.close()
        print("✅ Conexiones cerradas")

def main():
    success = fix_ganancias()
    if success:
        print(f"\n🏆 ¡GANANCIAS CORREGIDAS EXITOSAMENTE!")
    else:
        print(f"\n❌ Error al corregir ganancias")

if __name__ == "__main__":
    main()