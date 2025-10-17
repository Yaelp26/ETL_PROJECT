#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Complete Processor - Módulo principal para el procesamiento ETL completo
"""

import mysql.connector
from pathlib import Path
from dotenv import load_dotenv
import os
from datetime import datetime
import calendar

class ETLCompleteProcessor:
    """Clase principal para el proceso ETL completo"""
    
    def __init__(self):
        # Cargar variables de entorno
        project_root = Path(__file__).parent.parent.parent
        load_dotenv(project_root / 'config' / '.env')
        
        self.config_origen = {
            'host': os.getenv('ORIGEN_HOST'),
            'user': os.getenv('ORIGEN_USER'),
            'password': os.getenv('ORIGEN_PASS'),
            'database': os.getenv('ORIGEN_DB'),
            'charset': 'utf8mb4',
            'buffered': True
        }
        
        self.config_destino = {
            'host': os.getenv('DESTINO_HOST'),
            'user': os.getenv('DESTINO_USER'),
            'password': os.getenv('DESTINO_PASS'),
            'database': os.getenv('DESTINO_DB'),
            'charset': 'utf8mb4',
            'buffered': True
        }
        
        self.stats = {}
    
    def load_dimensions(self):
        """Carga todas las dimensiones principales"""
        print("📊 CARGANDO DIMENSIONES PRINCIPALES")
        print("=" * 40)
        
        success_count = 0
        
        # Cargar cada dimensión
        if self.load_dim_clientes():
            success_count += 1
        if self.load_dim_empleados():
            success_count += 1
        if self.load_dim_proyectos():
            success_count += 1
        if self.load_dim_tareas():
            success_count += 1
        if self.load_dim_tiempo():
            success_count += 1
        if self.load_dim_finanzas():
            success_count += 1
        
        print(f"\n📊 Dimensiones cargadas: {success_count}/6")
        return success_count == 6
    
    def load_dim_clientes(self):
        """Carga la dimensión de clientes"""
        print("👥 Cargando dim_clientes...")
        
        try:
            conn_origen = mysql.connector.connect(**self.config_origen)
            conn_destino = mysql.connector.connect(**self.config_destino)
            
            cursor_origen = conn_origen.cursor(buffered=True)
            cursor_destino = conn_destino.cursor(buffered=True)
            
            # Limpiar tabla
            cursor_destino.execute("DELETE FROM dim_clientes")
            
            # Extraer clientes únicos de proyectos cerrados/cancelados
            cursor_origen.execute("""
                SELECT DISTINCT 
                    c.ID_Cliente,
                    c.NombreCliente,
                    c.Direccion,
                    c.Telefono,
                    c.Email
                FROM clientes c
                INNER JOIN contratos ct ON c.ID_Cliente = ct.ID_Cliente
                INNER JOIN proyectos p ON ct.ID_Contrato = p.ID_Contrato
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                ORDER BY c.ID_Cliente
            """)
            
            clientes = cursor_origen.fetchall()
            
            if clientes:
                query_insert = """
                    INSERT INTO dim_clientes 
                    (ID_clientes, NombreCliente, Direccion, Telefono, Email)
                    VALUES (%s, %s, %s, %s, %s)
                """
                cursor_destino.executemany(query_insert, clientes)
                conn_destino.commit()
                
                print(f"   ✅ {len(clientes)} clientes cargados")
                self.stats['clientes'] = len(clientes)
            
            cursor_origen.close()
            cursor_destino.close()
            conn_origen.close()
            conn_destino.close()
            
            return True
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False
    
    def load_dim_empleados(self):
        """Carga la dimensión de empleados"""
        print("👤 Cargando dim_empleados...")
        
        try:
            conn_origen = mysql.connector.connect(**self.config_origen)
            conn_destino = mysql.connector.connect(**self.config_destino)
            
            cursor_origen = conn_origen.cursor(buffered=True)
            cursor_destino = conn_destino.cursor(buffered=True)
            
            # Limpiar tabla
            cursor_destino.execute("DELETE FROM dim_empleados")
            
            # Extraer empleados que trabajaron en proyectos cerrados/cancelados
            cursor_origen.execute("""
                SELECT DISTINCT 
                    e.ID_Empleado,
                    e.NombreEmpleado,
                    e.Cargo,
                    e.Departamento,
                    e.Email,
                    e.Telefono
                FROM empleados e
                INNER JOIN asignaciones a ON e.ID_Empleado = a.ID_Empleado
                INNER JOIN tareas t ON a.ID_Tarea = t.ID_Tarea
                INNER JOIN proyectos p ON t.ID_Proyecto = p.ID_Proyecto
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                ORDER BY e.ID_Empleado
            """)
            
            empleados = cursor_origen.fetchall()
            
            if empleados:
                query_insert = """
                    INSERT INTO dim_empleados 
                    (ID_empleados, NombreEmpleado, Cargo, Departamento, Email, Telefono)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor_destino.executemany(query_insert, empleados)
                conn_destino.commit()
                
                print(f"   ✅ {len(empleados)} empleados cargados")
                self.stats['empleados'] = len(empleados)
            
            cursor_origen.close()
            cursor_destino.close()
            conn_origen.close()
            conn_destino.close()
            
            return True
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False
    
    def load_dim_proyectos(self):
        """Carga la dimensión de proyectos"""
        print("📂 Cargando dim_proyectos...")
        
        try:
            conn_origen = mysql.connector.connect(**self.config_origen)
            conn_destino = mysql.connector.connect(**self.config_destino)
            
            cursor_origen = conn_origen.cursor(buffered=True)
            cursor_destino = conn_destino.cursor(buffered=True)
            
            # Limpiar tabla
            cursor_destino.execute("DELETE FROM dim_proyectos")
            
            # Extraer proyectos cerrados/cancelados
            cursor_origen.execute("""
                SELECT 
                    p.ID_Proyecto,
                    p.NombreProyecto,
                    p.Version,
                    p.FechaInicio,
                    p.FechaFin,
                    p.Estado,
                    p.CostoPresupuestado,
                    p.CostoReal
                FROM proyectos p
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                ORDER BY p.ID_Proyecto
            """)
            
            proyectos = cursor_origen.fetchall()
            
            if proyectos:
                query_insert = """
                    INSERT INTO dim_proyectos 
                    (ID_proyectos, CodigoProyecto, NombreProyecto, Version, 
                     FechaInicio, FechaFin, Estado, CostoPresupuestado, CostoReal)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor_destino.executemany(query_insert, proyectos)
                conn_destino.commit()
                
                print(f"   ✅ {len(proyectos)} proyectos cargados")
                self.stats['proyectos'] = len(proyectos)
            
            cursor_origen.close()
            cursor_destino.close()
            conn_origen.close()
            conn_destino.close()
            
            return True
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False
    
    def load_dim_tareas(self):
        """Carga la dimensión de tareas"""
        print("📋 Cargando dim_tareas...")
        
        try:
            conn_origen = mysql.connector.connect(**self.config_origen)
            conn_destino = mysql.connector.connect(**self.config_destino)
            
            cursor_origen = conn_origen.cursor(buffered=True)
            cursor_destino = conn_destino.cursor(buffered=True)
            
            # Limpiar tabla
            cursor_destino.execute("DELETE FROM dim_tareas")
            
            # Extraer tareas de proyectos cerrados/cancelados
            cursor_origen.execute("""
                SELECT 
                    t.ID_Tarea,
                    t.ID_Proyecto,
                    t.NombreTarea,
                    t.Descripcion,
                    t.Estado,
                    t.DuracionPlanificada,
                    t.DuracionReal
                FROM tareas t
                INNER JOIN proyectos p ON t.ID_Proyecto = p.ID_Proyecto
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                ORDER BY t.ID_Tarea
            """)
            
            tareas = cursor_origen.fetchall()
            
            if tareas:
                query_insert = """
                    INSERT INTO dim_tareas 
                    (ID_tareas, ID_Proyecto, NombreTarea, Descripcion, 
                     Estado, DuracionPlanificada, DuracionReal)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor_destino.executemany(query_insert, tareas)
                conn_destino.commit()
                
                print(f"   ✅ {len(tareas)} tareas cargadas")
                self.stats['tareas'] = len(tareas)
            
            cursor_origen.close()
            cursor_destino.close()
            conn_origen.close()
            conn_destino.close()
            
            return True
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False
    
    def load_dim_tiempo(self):
        """Carga la dimensión de tiempo"""
        print("📅 Cargando dim_tiempo...")
        
        try:
            conn_origen = mysql.connector.connect(**self.config_origen)
            conn_destino = mysql.connector.connect(**self.config_destino)
            
            cursor_origen = conn_origen.cursor(buffered=True)
            cursor_destino = conn_destino.cursor(buffered=True)
            
            # Limpiar tabla
            cursor_destino.execute("DELETE FROM dim_tiempo")
            
            # Extraer fechas únicas
            cursor_origen.execute("""
                SELECT DISTINCT DATE(p.FechaInicio) as fecha
                FROM proyectos p
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                UNION
                SELECT DISTINCT DATE(p.FechaFin) as fecha
                FROM proyectos p
                WHERE p.Estado IN ('Cancelado', 'Cerrado') AND p.FechaFin IS NOT NULL
                ORDER BY fecha
            """)
            
            fechas = [row[0] for row in cursor_origen.fetchall()]
            
            if fechas:
                datos_tiempo = []
                
                for fecha in fechas:
                    fecha_dt = datetime.combine(fecha, datetime.min.time()) if hasattr(fecha, 'year') else datetime.strptime(str(fecha), '%Y-%m-%d')
                    
                    id_tiempo = int(fecha_dt.strftime('%Y%m%d'))
                    año = fecha_dt.year
                    mes = fecha_dt.month
                    dia_semana = fecha_dt.weekday() + 1
                    
                    datos_tiempo.append((id_tiempo, fecha, dia_semana, mes, año))
                
                query_insert = """
                    INSERT IGNORE INTO dim_tiempo 
                    (ID_Tiempo, Fecha, ID_Dia, ID_Mes, ID_Anio)
                    VALUES (%s, %s, %s, %s, %s)
                """
                cursor_destino.executemany(query_insert, datos_tiempo)
                conn_destino.commit()
                
                print(f"   ✅ {len(datos_tiempo)} fechas cargadas")
                self.stats['tiempo'] = len(datos_tiempo)
            
            cursor_origen.close()
            cursor_destino.close()
            conn_origen.close()
            conn_destino.close()
            
            return True
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False
    
    def load_dim_finanzas(self):
        """Carga la dimensión de finanzas"""
        print("💰 Cargando dim_finanzas...")
        
        try:
            conn_origen = mysql.connector.connect(**self.config_origen)
            conn_destino = mysql.connector.connect(**self.config_destino)
            
            cursor_origen = conn_origen.cursor(buffered=True)
            cursor_destino = conn_destino.cursor(buffered=True)
            
            # Limpiar tabla
            cursor_destino.execute("DELETE FROM dim_finanzas")
            
            # Extraer datos financieros de proyectos y contratos
            cursor_origen.execute("""
                SELECT DISTINCT
                    ROW_NUMBER() OVER (ORDER BY p.ID_Proyecto, c.ID_Contrato) as ID_Finanzas,
                    p.ID_Proyecto,
                    c.ID_Contrato,
                    c.ValorTotalContrato,
                    p.CostoPresupuestado,
                    p.CostoReal,
                    (c.ValorTotalContrato - p.CostoReal) as Ganancias,
                    'Proyecto' as TipoFinanza,
                    c.FechaContrato as FechaTransaccion
                FROM proyectos p
                INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                  AND c.Estado IN ('Cerrado', 'Cancelado')
                ORDER BY p.ID_Proyecto
            """)
            
            finanzas = cursor_origen.fetchall()
            
            if finanzas:
                query_insert = """
                    INSERT INTO dim_finanzas 
                    (ID_Finanzas, ID_Proyecto, ID_Contrato, ValorContrato, 
                     CostoPresupuestado, CostoReal, Ganancias, TipoFinanza, FechaTransaccion)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor_destino.executemany(query_insert, finanzas)
                conn_destino.commit()
                
                print(f"   ✅ {len(finanzas)} registros financieros cargados")
                self.stats['finanzas'] = len(finanzas)
            
            cursor_origen.close()
            cursor_destino.close()
            conn_origen.close()
            conn_destino.close()
            
            return True
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False
    
    def load_fact_table(self):
        """Carga la tabla de hechos"""
        print("\n🎯 CARGANDO TABLA DE HECHOS")
        print("=" * 40)
        
        try:
            conn_origen = mysql.connector.connect(**self.config_origen)
            conn_destino = mysql.connector.connect(**self.config_destino)
            
            cursor_origen = conn_origen.cursor(buffered=True)
            cursor_destino = conn_destino.cursor(buffered=True)
            
            # Limpiar tabla de hechos
            cursor_destino.execute("DELETE FROM hechos_proyectos")
            
            # Obtener mapeos de dimensiones
            print("📊 Obteniendo mapeos de dimensiones...")
            
            # Mapeo proyectos
            cursor_destino.execute("SELECT ID_proyectos, CodigoProyecto FROM dim_proyectos")
            mapeo_proyectos = {row[1]: row[0] for row in cursor_destino.fetchall()}
            
            # Mapeo tiempo
            cursor_destino.execute("SELECT ID_Tiempo, Fecha FROM dim_tiempo")
            mapeo_tiempo = {row[1]: row[0] for row in cursor_destino.fetchall()}
            
            # Mapeo finanzas
            cursor_destino.execute("SELECT ID_Finanzas, ID_Proyecto FROM dim_finanzas")
            mapeo_finanzas = {row[1]: row[0] for row in cursor_destino.fetchall()}
            
            # Extraer datos para tabla de hechos
            cursor_origen.execute("""
                SELECT 
                    p.ID_Proyecto,
                    DATE(p.FechaInicio) as FechaInicio,
                    DATE(p.FechaFin) as FechaFin,
                    p.CostoPresupuestado,
                    p.CostoReal,
                    c.ValorTotalContrato,
                    (c.ValorTotalContrato - p.CostoReal) as Ganancias,
                    p.Estado,
                    COUNT(DISTINCT t.ID_Tarea) as TotalTareas,
                    COUNT(DISTINCT a.ID_Empleado) as TotalEmpleados
                FROM proyectos p
                INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
                LEFT JOIN tareas t ON p.ID_Proyecto = t.ID_Proyecto
                LEFT JOIN asignaciones a ON t.ID_Tarea = a.ID_Tarea
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                  AND c.Estado IN ('Cerrado', 'Cancelado')
                GROUP BY p.ID_Proyecto, p.FechaInicio, p.FechaFin, 
                         p.CostoPresupuestado, p.CostoReal, c.ValorTotalContrato, p.Estado
                ORDER BY p.ID_Proyecto
            """)
            
            datos_hechos = cursor_origen.fetchall()
            
            if datos_hechos:
                hechos_procesados = []
                
                for fila in datos_hechos:
                    id_proyecto = fila[0]
                    fecha_inicio = fila[1]
                    fecha_fin = fila[2]
                    costo_presupuestado = fila[3]
                    costo_real = fila[4]
                    valor_contrato = fila[5]
                    ganancias = fila[6]
                    estado = fila[7]
                    total_tareas = fila[8]
                    total_empleados = fila[9]
                    
                    # Obtener IDs de dimensiones
                    id_dim_proyecto = mapeo_proyectos.get(id_proyecto)
                    id_tiempo_inicio = mapeo_tiempo.get(fecha_inicio)
                    id_tiempo_fin = mapeo_tiempo.get(fecha_fin) if fecha_fin else None
                    id_finanzas = mapeo_finanzas.get(id_proyecto)
                    
                    if id_dim_proyecto and id_tiempo_inicio and id_finanzas:
                        hechos_procesados.append((
                            id_dim_proyecto,    # ID_proyectos
                            id_tiempo_inicio,   # ID_TiempoInicio
                            id_tiempo_fin,      # ID_TiempoFin
                            id_finanzas,        # ID_Finanzas
                            costo_presupuestado,
                            costo_real,
                            valor_contrato,
                            ganancias,
                            total_tareas,
                            total_empleados
                        ))
                
                if hechos_procesados:
                    query_insert = """
                        INSERT INTO hechos_proyectos 
                        (ID_proyectos, ID_TiempoInicio, ID_TiempoFin, ID_Finanzas,
                         CostoPresupuestado, CostoReal, ValorContrato, Ganancias,
                         TotalTareas, TotalEmpleados)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor_destino.executemany(query_insert, hechos_procesados)
                    conn_destino.commit()
                    
                    print(f"✅ {len(hechos_procesados)} registros de hechos cargados")
                    self.stats['hechos'] = len(hechos_procesados)
                else:
                    print("❌ No se pudieron procesar los datos de hechos")
            
            cursor_origen.close()
            cursor_destino.close()
            conn_origen.close()
            conn_destino.close()
            
            return True
            
        except Exception as e:
            print(f"❌ Error cargando tabla de hechos: {e}")
            return False
    
    def run_complete_etl(self):
        """Ejecuta el proceso ETL completo"""
        print("🚀 INICIANDO PROCESO ETL COMPLETO")
        print("=" * 50)
        
        start_time = datetime.now()
        
        # 1. Cargar dimensiones
        if not self.load_dimensions():
            print("❌ Error cargando dimensiones. Proceso abortado.")
            return False
        
        # 2. Cargar tabla de hechos
        if not self.load_fact_table():
            print("❌ Error cargando tabla de hechos. Proceso abortado.")
            return False
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Resumen final
        print(f"\n🎉 ¡PROCESO ETL COMPLETADO EXITOSAMENTE!")
        print("=" * 50)
        print(f"⏱️  Duración: {duration}")
        print(f"📊 Estadísticas:")
        for tabla, count in self.stats.items():
            print(f"   ✅ {tabla}: {count} registros")
        
        total_records = sum(self.stats.values())
        print(f"📈 Total registros procesados: {total_records:,}")
        
        return True

if __name__ == "__main__":
    processor = ETLCompleteProcessor()
    processor.run_complete_etl()