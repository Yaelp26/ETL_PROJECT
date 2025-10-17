#!/usr/bin/env python3
"""
Proceso ETL Principal - Versión Completa Corregida
"""

import sys
import os
sys.path.append('scripts')

from src.db_connector import get_db_connection
from datetime import datetime

class ETLProcessor:
    def __init__(self):
        self.source_conn = None
        self.dest_conn = None
        self.stats = {}
    
    def connect(self):
        """Establece conexiones con ambas bases de datos."""
        self.source_conn = get_db_connection('origen')
        self.dest_conn = get_db_connection('destino')
        
        if not self.source_conn or not self.dest_conn:
            print("❌ Error: No se pudieron establecer las conexiones")
            return False
        
        print("✅ Conexiones establecidas exitosamente")
        return True
    
    def disconnect(self):
        """Cierra las conexiones."""
        if self.source_conn and self.source_conn.is_connected():
            self.source_conn.close()
        if self.dest_conn and self.dest_conn.is_connected():
            self.dest_conn.close()
        print("✅ Conexiones cerradas")
    
    def load_dim_tiempo(self):
        """3. Carga tabla dim_tiempo procesando fechas de proyectos (CORREGIDO)."""
        print("\n📅 3. Cargando dim_tiempo...")
        
        try:
            with self.source_conn.cursor() as source_cursor, self.dest_conn.cursor() as dest_cursor:
                
                # Primero, asegurar que tenemos todos los años necesarios
                source_cursor.execute("""
                    SELECT DISTINCT YEAR(FechaInicio) as anio FROM proyectos p
                    INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
                    WHERE p.Estado IN ('Cancelado', 'Cerrado') AND c.Estado IN ('Cerrado', 'Cancelado')
                    AND FechaInicio IS NOT NULL
                    UNION
                    SELECT DISTINCT YEAR(FechaFin) as anio FROM proyectos p
                    INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
                    WHERE p.Estado IN ('Cancelado', 'Cerrado') AND c.Estado IN ('Cerrado', 'Cancelado')
                    AND FechaFin IS NOT NULL
                """)
                
                años_necesarios = [row[0] for row in source_cursor.fetchall()]
                
                # Insertar años faltantes
                for año in años_necesarios:
                    dest_cursor.execute(
                        "INSERT IGNORE INTO subdim_anio (NumeroAnio) VALUES (%s)",
                        (año,)
                    )
                
                # Ahora extraer fechas únicas
                query = """
                SELECT DISTINCT FechaInicio as Fecha FROM proyectos p
                INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                AND c.Estado IN ('Cerrado', 'Cancelado')
                AND FechaInicio IS NOT NULL
                UNION
                SELECT DISTINCT FechaFin as Fecha FROM proyectos p
                INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                AND c.Estado IN ('Cerrado', 'Cancelado')
                AND FechaFin IS NOT NULL
                ORDER BY Fecha
                """
                
                source_cursor.execute(query)
                fechas = source_cursor.fetchall()
                
                # Procesar cada fecha
                fechas_insertadas = 0
                for fecha_tuple in fechas:
                    fecha = fecha_tuple[0]
                    
                    # Obtener IDs de subdimensiones (con manejo de errores)
                    dest_cursor.execute("SELECT ID_Dia FROM subdim_dia WHERE NumeroDiaSemana = %s", (fecha.isoweekday(),))
                    id_dia_result = dest_cursor.fetchone()
                    if not id_dia_result:
                        continue
                    id_dia = id_dia_result[0]
                    
                    dest_cursor.execute("SELECT ID_Mes FROM subdim_mes WHERE NumeroMes = %s", (fecha.month,))
                    id_mes_result = dest_cursor.fetchone()
                    if not id_mes_result:
                        continue
                    id_mes = id_mes_result[0]
                    
                    dest_cursor.execute("SELECT ID_Anio FROM subdim_anio WHERE NumeroAnio = %s", (fecha.year,))
                    id_anio_result = dest_cursor.fetchone()
                    if not id_anio_result:
                        continue
                    id_anio = id_anio_result[0]
                    
                    # Insertar fecha
                    dest_cursor.execute(
                        """INSERT IGNORE INTO dim_tiempo 
                           (Fecha, ID_Dia, ID_Mes, ID_Anio) 
                           VALUES (%s, %s, %s, %s)""",
                        (fecha, id_dia, id_mes, id_anio)
                    )
                    fechas_insertadas += 1
                
                self.dest_conn.commit()
                self.stats['dim_tiempo'] = fechas_insertadas
                print(f"   ✅ Insertadas {fechas_insertadas} fechas únicas")
                
        except Exception as e:
            print(f"   ❌ Error cargando dim_tiempo: {e}")
            self.dest_conn.rollback()
    
    def load_dim_proyectos(self):
        """4. Carga tabla dim_proyectos con referencias a clientes."""
        print("\n📊 4. Cargando dim_proyectos...")
        
        try:
            with self.source_conn.cursor() as source_cursor, self.dest_conn.cursor() as dest_cursor:
                
                # Extraer proyectos filtrados con referencias a clientes
                query = """
                SELECT DISTINCT 
                    p.ID_Proyecto,
                    p.Estado,
                    p.CostoPresupuestado,
                    p.CostoReal,
                    dc.ID_Cliente as DW_ClienteID
                FROM proyectos p
                INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
                INNER JOIN clientes cli ON c.ID_Cliente = cli.ID_Cliente
                INNER JOIN dim_clientes dc ON cli.ID_Cliente = dc.CodigoClienteReal
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                AND c.Estado IN ('Cerrado', 'Cancelado')
                """
                
                source_cursor.execute(query)
                proyectos = source_cursor.fetchall()
                
                # Insertar en destino
                for proyecto in proyectos:
                    dest_cursor.execute(
                        """INSERT IGNORE INTO dim_proyectos 
                           (CodigoProyecto, Estado, CostoPresupuestado, CostoReal, ID_Cliente) 
                           VALUES (%s, %s, %s, %s, %s)""",
                        (str(proyecto[0]), proyecto[1], proyecto[2], proyecto[3], proyecto[4])
                    )
                
                self.dest_conn.commit()
                self.stats['dim_proyectos'] = len(proyectos)
                print(f"   ✅ Insertados {len(proyectos)} proyectos")
                
        except Exception as e:
            print(f"   ❌ Error cargando dim_proyectos: {e}")
            self.dest_conn.rollback()
    
    def load_dim_tareas(self):
        """5. Carga tabla dim_tareas con referencias a proyectos."""
        print("\n📝 5. Cargando dim_tareas...")
        
        try:
            with self.source_conn.cursor() as source_cursor, self.dest_conn.cursor() as dest_cursor:
                
                # Extraer tareas de proyectos filtrados
                query = """
                SELECT DISTINCT 
                    t.ID_Tarea,
                    t.DuracionPlanificada,
                    t.DuracionReal,
                    dp.ID_proyectos as DW_ProyectoID
                FROM tareas t
                INNER JOIN proyectos p ON t.ID_Proyecto = p.ID_Proyecto
                INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
                INNER JOIN dim_proyectos dp ON p.ID_Proyecto = CAST(dp.CodigoProyecto AS SIGNED)
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                AND c.Estado IN ('Cerrado', 'Cancelado')
                """
                
                source_cursor.execute(query)
                tareas = source_cursor.fetchall()
                
                # Insertar en destino
                for tarea in tareas:
                    dest_cursor.execute(
                        """INSERT IGNORE INTO dim_tareas 
                           (CodigoTarea, DuracionPlanificadaWeek, DuracionRealWeek, dim_proyectos_ID_proyectos) 
                           VALUES (%s, %s, %s, %s)""",
                        (tarea[0], tarea[1], tarea[2], tarea[3])
                    )
                
                self.dest_conn.commit()
                self.stats['dim_tareas'] = len(tareas)
                print(f"   ✅ Insertadas {len(tareas)} tareas")
                
        except Exception as e:
            print(f"   ❌ Error cargando dim_tareas: {e}")
            self.dest_conn.rollback()
    
    def load_dim_hitos(self):
        """6. Carga tabla dim_hitos con cálculo de retrasos."""
        print("\n🎯 6. Cargando dim_hitos...")
        
        try:
            with self.source_conn.cursor() as source_cursor, self.dest_conn.cursor() as dest_cursor:
                
                # Extraer hitos de proyectos filtrados con cálculo de retraso
                query = """
                SELECT DISTINCT 
                    h.ID_Hito,
                    dp.ID_proyectos as DW_ProyectoID,
                    DATEDIFF(h.FechaFinReal, h.FechaInicioPlanificada) as Retraso_days
                FROM hitos h
                INNER JOIN proyectos p ON h.ID_Proyecto = p.ID_Proyecto
                INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
                INNER JOIN dim_proyectos dp ON p.ID_Proyecto = CAST(dp.CodigoProyecto AS SIGNED)
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                AND c.Estado IN ('Cerrado', 'Cancelado')
                AND h.FechaFinReal IS NOT NULL 
                AND h.FechaInicioPlanificada IS NOT NULL
                """
                
                source_cursor.execute(query)
                hitos = source_cursor.fetchall()
                
                # Insertar en destino
                for hito in hitos:
                    dest_cursor.execute(
                        """INSERT IGNORE INTO dim_hitos 
                           (CodigoHito, ID_proyectos, Retraso_days) 
                           VALUES (%s, %s, %s)""",
                        (str(hito[0]), hito[1], hito[2])
                    )
                
                self.dest_conn.commit()
                self.stats['dim_hitos'] = len(hitos)
                print(f"   ✅ Insertados {len(hitos)} hitos")
                
        except Exception as e:
            print(f"   ❌ Error cargando dim_hitos: {e}")
            self.dest_conn.rollback()
    
    def load_dim_finanzas_data(self):
        """7. Carga datos reales en dim_finanzas (gastos y penalizaciones)."""
        print("\n💰 7. Cargando datos financieros...")
        
        try:
            with self.source_conn.cursor() as source_cursor, self.dest_conn.cursor() as dest_cursor:
                
                # Limpiar datos previos (mantener solo catálogos)
                dest_cursor.execute("DELETE FROM dim_finanzas WHERE Monto > 0")
                
                # Cargar gastos de proyectos filtrados
                query_gastos = """
                SELECT 
                    g.TipoGasto,
                    g.Monto
                FROM gastos g
                INNER JOIN proyectos p ON g.ID_Proyecto = p.ID_Proyecto
                INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                AND c.Estado IN ('Cerrado', 'Cancelado')
                """
                
                source_cursor.execute(query_gastos)
                gastos = source_cursor.fetchall()
                
                gastos_insertados = 0
                for gasto in gastos:
                    dest_cursor.execute(
                        """INSERT INTO dim_finanzas (TipoGasto, Monto) VALUES (%s, %s)""",
                        (gasto[0], gasto[1])
                    )
                    gastos_insertados += 1
                
                # Cargar penalizaciones
                query_penalizaciones = """
                SELECT 
                    'Penalizacion' as TipoGasto,
                    pen.Monto
                FROM penalizaciones pen
                INNER JOIN contratos c ON pen.ID_Contrato = c.ID_Contrato
                INNER JOIN proyectos p ON c.ID_Contrato = p.ID_Contrato
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                AND c.Estado IN ('Cerrado', 'Cancelado')
                """
                
                source_cursor.execute(query_penalizaciones)
                penalizaciones = source_cursor.fetchall()
                
                penalizaciones_insertadas = 0
                for pen in penalizaciones:
                    dest_cursor.execute(
                        """INSERT INTO dim_finanzas (TipoGasto, Monto) VALUES (%s, %s)""",
                        (pen[0], pen[1])
                    )
                    penalizaciones_insertadas += 1
                
                self.dest_conn.commit()
                total_finanzas = gastos_insertados + penalizaciones_insertadas
                self.stats['dim_finanzas_data'] = total_finanzas
                print(f"   ✅ Insertados {gastos_insertados} gastos + {penalizaciones_insertadas} penalizaciones = {total_finanzas} registros financieros")
                
        except Exception as e:
            print(f"   ❌ Error cargando datos financieros: {e}")
            self.dest_conn.rollback()
    
    def run_etl_complete(self):
        """Ejecuta el proceso ETL completo con todas las dimensiones."""
        print("🚀 EJECUTANDO PROCESO ETL COMPLETO")
        print("=" * 50)
        
        if not self.connect():
            return False
        
        try:
            # Orden de carga según dependencias
            print("📋 Fase 1: Dimensiones independientes...")
            # dim_clientes y dim_empleados ya están cargados
            
            print("\n📋 Fase 2: Dimensiones dependientes...")
            self.load_dim_tiempo()  # Corregido
            self.load_dim_proyectos()
            self.load_dim_tareas()
            self.load_dim_hitos()
            self.load_dim_finanzas_data()
            
            print(f"\n📊 RESUMEN FINAL DE DIMENSIONES:")
            for tabla, count in self.stats.items():
                print(f"   📋 {tabla}: {count:,} registros")
            
            print(f"\n🎉 ¡Todas las dimensiones cargadas exitosamente!")
            return True
            
        except Exception as e:
            print(f"❌ Error en el proceso ETL: {e}")
            return False
        finally:
            self.disconnect()

def main():
    etl = ETLProcessor()
    success = etl.run_etl_complete()
    
    if success:
        print(f"\n🚀 Proceso ETL de dimensiones completado con éxito!")
        print(f"📋 Siguiente paso: Implementar tabla de hechos")
    else:
        print(f"\n❌ El proceso ETL falló.")

if __name__ == "__main__":
    main()