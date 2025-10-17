#!/usr/bin/env python3
"""
Proceso ETL Principal - Implementación completa basada en mapeo de datos
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
    
    def load_dim_clientes(self):
        """1. Carga tabla dim_clientes desde tabla clientes."""
        print("\n📋 1. Cargando dim_clientes...")
        
        try:
            with self.source_conn.cursor() as source_cursor, self.dest_conn.cursor() as dest_cursor:
                
                # Extraer clientes únicos de proyectos filtrados
                query = """
                SELECT DISTINCT 
                    cli.ID_Cliente, 
                    cli.NombreCliente
                FROM clientes cli
                INNER JOIN contratos c ON cli.ID_Cliente = c.ID_Cliente
                INNER JOIN proyectos p ON c.ID_Contrato = p.ID_Contrato
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                AND c.Estado IN ('Cerrado', 'Cancelado')
                """
                
                source_cursor.execute(query)
                clientes = source_cursor.fetchall()
                
                # Insertar en destino
                for cliente in clientes:
                    dest_cursor.execute(
                        "INSERT IGNORE INTO dim_clientes (CodigoClienteReal, NombreCliente) VALUES (%s, %s)",
                        (cliente[0], cliente[1])
                    )
                
                self.dest_conn.commit()
                self.stats['dim_clientes'] = len(clientes)
                print(f"   ✅ Insertados {len(clientes)} clientes")
                
        except Exception as e:
            print(f"   ❌ Error cargando dim_clientes: {e}")
            self.dest_conn.rollback()
    
    def load_dim_empleados(self):
        """2. Carga tabla dim_empleados con cálculo de salario."""
        print("\n👥 2. Cargando dim_empleados...")
        
        try:
            with self.source_conn.cursor() as source_cursor, self.dest_conn.cursor() as dest_cursor:
                
                # Extraer empleados que trabajaron en proyectos filtrados
                query = """
                SELECT DISTINCT 
                    e.ID_Empleado,
                    e.NombreCompleto,
                    e.Rol,
                    e.CostoPorHora * e.HorasTrabajo as Salario
                FROM empleados e
                INNER JOIN asignaciones a ON e.ID_Empleado = a.empleados_ID_Empleado
                INNER JOIN tareas t ON a.tareas_ID_Tarea = t.ID_Tarea
                INNER JOIN proyectos p ON t.ID_Proyecto = p.ID_Proyecto
                INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                AND c.Estado IN ('Cerrado', 'Cancelado')
                """
                
                source_cursor.execute(query)
                empleados = source_cursor.fetchall()
                
                # Insertar en destino
                for empleado in empleados:
                    dest_cursor.execute(
                        """INSERT IGNORE INTO dim_empleados 
                           (CodigoEmpleado, NombreEmpleado, Rol, Salario) 
                           VALUES (%s, %s, %s, %s)""",
                        (empleado[0], empleado[1], empleado[2], empleado[3])
                    )
                
                self.dest_conn.commit()
                self.stats['dim_empleados'] = len(empleados)
                print(f"   ✅ Insertados {len(empleados)} empleados")
                
        except Exception as e:
            print(f"   ❌ Error cargando dim_empleados: {e}")
            self.dest_conn.rollback()
    
    def load_dim_tiempo(self):
        """3. Carga tabla dim_tiempo procesando fechas de proyectos."""
        print("\n📅 3. Cargando dim_tiempo...")
        
        try:
            with self.source_conn.cursor() as source_cursor, self.dest_conn.cursor() as dest_cursor:
                
                # Extraer fechas únicas de proyectos filtrados
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
                for fecha_tuple in fechas:
                    fecha = fecha_tuple[0]
                    
                    # Obtener IDs de subdimensiones
                    dest_cursor.execute("SELECT ID_Dia FROM subdim_dia WHERE NumeroDiaSemana = %s", (fecha.isoweekday(),))
                    id_dia = dest_cursor.fetchone()[0]
                    
                    dest_cursor.execute("SELECT ID_Mes FROM subdim_mes WHERE NumeroMes = %s", (fecha.month,))
                    id_mes = dest_cursor.fetchone()[0]
                    
                    dest_cursor.execute("SELECT ID_Anio FROM subdim_anio WHERE NumeroAnio = %s", (fecha.year,))
                    id_anio = dest_cursor.fetchone()[0]
                    
                    # Insertar fecha
                    dest_cursor.execute(
                        """INSERT IGNORE INTO dim_tiempo 
                           (Fecha, ID_Dia, ID_Mes, ID_Anio) 
                           VALUES (%s, %s, %s, %s)""",
                        (fecha, id_dia, id_mes, id_anio)
                    )
                
                self.dest_conn.commit()
                self.stats['dim_tiempo'] = len(fechas)
                print(f"   ✅ Insertadas {len(fechas)} fechas únicas")
                
        except Exception as e:
            print(f"   ❌ Error cargando dim_tiempo: {e}")
            self.dest_conn.rollback()
    
    def run_etl(self):
        """Ejecuta el proceso ETL completo."""
        print("🚀 INICIANDO PROCESO ETL PRINCIPAL")
        print("=" * 50)
        
        if not self.connect():
            return False
        
        try:
            # Orden de carga según dependencias
            self.load_dim_clientes()
            self.load_dim_empleados()
            self.load_dim_tiempo()
            
            print(f"\n📊 RESUMEN DE CARGA:")
            for tabla, count in self.stats.items():
                print(f"   📋 {tabla}: {count} registros")
            
            print(f"\n🎉 ¡ETL Principal completado exitosamente!")
            return True
            
        except Exception as e:
            print(f"❌ Error en el proceso ETL: {e}")
            return False
        finally:
            self.disconnect()

def main():
    etl = ETLProcessor()
    success = etl.run_etl()
    
    if success:
        print(f"\n🚀 Proceso ETL completado con éxito!")
    else:
        print(f"\n❌ El proceso ETL falló.")

if __name__ == "__main__":
    main()