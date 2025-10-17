#!/usr/bin/env python3
"""
ETL Final - Implementación completa con tabla de hechos
"""

import sys
import os
sys.path.append('scripts')

from src.db_connector import get_db_connection

class ETLFinal:
    def __init__(self):
        self.source_conn = None
        self.dest_conn = None
        self.stats = {}
    
    def connect(self):
        """Establece conexiones."""
        self.source_conn = get_db_connection('origen')
        self.dest_conn = get_db_connection('destino')
        
        if not self.source_conn or not self.dest_conn:
            print("❌ Error: No se pudieron establecer las conexiones")
            return False
        
        print("✅ Conexiones establecidas")
        return True
    
    def disconnect(self):
        """Cierra conexiones."""
        if self.source_conn and self.source_conn.is_connected():
            self.source_conn.close()
        if self.dest_conn and self.dest_conn.is_connected():
            self.dest_conn.close()
        print("✅ Conexiones cerradas")
    
    def load_hechos_proyectos(self):
        """Carga la tabla de hechos con todas las métricas agregadas."""
        print("\n🎯 Cargando tabla de hechos_proyectos...")
        
        try:
            source_cursor = self.source_conn.cursor()
            dest_cursor = self.dest_conn.cursor()
            
            # Obtener mapeos del DW
            dest_cursor.execute("SELECT ID_proyectos, CodigoProyecto FROM dim_proyectos")
            mapeo_proyectos = {int(row[1]): row[0] for row in dest_cursor.fetchall()}
            
            dest_cursor.execute("SELECT ID_Tiempo, Fecha FROM dim_tiempo")
            mapeo_fechas = {row[1]: row[0] for row in dest_cursor.fetchall()}
            
            print(f"   📋 Mapeos cargados: {len(mapeo_proyectos)} proyectos, {len(mapeo_fechas)} fechas")
            
            # Query principal para extraer todos los datos agregados por proyecto
            query_hechos = """
            SELECT 
                p.ID_Proyecto,
                p.FechaInicio,
                p.FechaFin,
                p.CostoPresupuestado,
                p.CostoReal,
                c.ValorTotalContrato,
                
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
            
            source_cursor.execute(query_hechos)
            hechos_data = source_cursor.fetchall()
            source_cursor.close()
            
            print(f"   📊 Datos extraídos: {len(hechos_data)} registros de hechos")
            
            # Insertar en tabla de hechos
            hechos_insertados = 0
            for hecho in hechos_data:
                proyecto_id = hecho[0]
                fecha_inicio = hecho[1]
                fecha_fin = hecho[2]
                costo_presupuestado = hecho[3]
                costo_real = hecho[4]
                valor_contrato = hecho[5]
                horas_planificadas = hecho[6]
                horas_reales = hecho[7]
                defectos = hecho[8]
                tests_exitosos = hecho[9]
                
                # Calcular ganancias
                ganancias = float(valor_contrato) - float(costo_real) if valor_contrato and costo_real else 0.0
                
                # Obtener IDs del DW
                proyecto_dw_id = mapeo_proyectos.get(proyecto_id)
                fecha_inicio_id = mapeo_fechas.get(fecha_inicio)
                fecha_fin_id = mapeo_fechas.get(fecha_fin)
                
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
            self.dest_conn.commit()
            self.stats['hechos_proyectos'] = hechos_insertados
            print(f"   ✅ Insertados {hechos_insertados} registros en tabla de hechos")
                
        except Exception as e:
            print(f"   ❌ Error cargando hechos_proyectos: {e}")
            self.dest_conn.rollback()
    
    def verify_data_warehouse(self):
        """Verifica el estado final del Data Warehouse."""
        print("\n📊 VERIFICACIÓN FINAL DEL DATA WAREHOUSE")
        print("=" * 50)
        
        try:
            dest_cursor = self.dest_conn.cursor()
            
            # Verificar todas las tablas
            tablas_dw = [
                'dim_clientes', 'dim_empleados', 'dim_proyectos', 'dim_tareas',
                'dim_finanzas', 'dim_tiempo', 'hechos_proyectos'
            ]
            
            total_registros = 0
            for tabla in tablas_dw:
                dest_cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
                count = dest_cursor.fetchone()[0]
                total_registros += count
                print(f"   📋 {tabla}: {count:,} registros")
            
            print(f"\n🎯 TOTAL REGISTROS EN DW: {total_registros:,}")
            
            # Verificar algunos registros de hechos
            print(f"\n🔍 MUESTRA DE TABLA DE HECHOS:")
            dest_cursor.execute("""
                SELECT h.ID_Hecho, h.ID_proyecto, h.HorasPlanificadas, h.HorasReales, 
                       h.CostoPresupuestado, h.CostoReal, h.Ganancias
                FROM hechos_proyectos h 
                LIMIT 5
            """)
            
            muestra_hechos = dest_cursor.fetchall()
            for i, hecho in enumerate(muestra_hechos, 1):
                print(f"   {i}. Proyecto {hecho[1]}: Horas P/R: {hecho[2]}/{hecho[3]}, Costos P/R: {hecho[4]}/{hecho[5]}, Ganancia: {hecho[6]}")
            
            dest_cursor.close()
            
        except Exception as e:
            print(f"❌ Error en verificación: {e}")
    
    def run_complete_etl(self):
        """Ejecuta el ETL completo incluyendo tabla de hechos."""
        print("🚀 EJECUTANDO ETL COMPLETO FINAL")
        print("=" * 50)
        
        if not self.connect():
            return False
        
        try:
            self.load_hechos_proyectos()
            self.verify_data_warehouse()
            
            print(f"\n🎉 ¡ETL COMPLETO FINALIZADO CON ÉXITO!")
            print(f"📊 Data Warehouse listo para análisis de inteligencia de negocios")
            return True
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
        finally:
            self.disconnect()

def main():
    etl = ETLFinal()
    success = etl.run_complete_etl()
    
    if success:
        print(f"\n🏆 ¡PROCESO ETL COMPLETADO AL 100%!")
        print(f"📋 El Data Warehouse está listo para consultas OLAP")
    else:
        print(f"\n❌ Falló el proceso final")

if __name__ == "__main__":
    main()