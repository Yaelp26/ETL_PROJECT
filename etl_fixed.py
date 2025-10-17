#!/usr/bin/env python3
"""
ETL Corregido - Manejo correcto de conexiones separadas
"""

import sys
import os
sys.path.append('scripts')

from src.db_connector import get_db_connection

class ETLProcessorFixed:
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
    
    def load_dim_tiempo_fixed(self):
        """Carga dim_tiempo corregido."""
        print("\n📅 Cargando dim_tiempo (corregido)...")
        
        try:
            source_cursor = self.source_conn.cursor()
            dest_cursor = self.dest_conn.cursor()
            
            # Primero obtener años únicos
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
            source_cursor.close()
            
            # Insertar años faltantes
            for año in años_necesarios:
                dest_cursor.execute(
                    "INSERT IGNORE INTO subdim_anio (NumeroAnio) VALUES (%s)",
                    (año,)
                )
            self.dest_conn.commit()
            
            # Ahora obtener fechas
            source_cursor = self.source_conn.cursor()
            source_cursor.execute("""
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
            """)
            
            fechas = source_cursor.fetchall()
            source_cursor.close()
            
            # Procesar fechas
            fechas_insertadas = 0
            for fecha_tuple in fechas:
                fecha = fecha_tuple[0]
                
                # Obtener IDs de subdimensiones
                dest_cursor.execute("SELECT ID_Dia FROM subdim_dia WHERE NumeroDiaSemana = %s", (fecha.isoweekday(),))
                id_dia_result = dest_cursor.fetchone()
                if not id_dia_result:
                    continue
                
                dest_cursor.execute("SELECT ID_Mes FROM subdim_mes WHERE NumeroMes = %s", (fecha.month,))
                id_mes_result = dest_cursor.fetchone()
                if not id_mes_result:
                    continue
                
                dest_cursor.execute("SELECT ID_Anio FROM subdim_anio WHERE NumeroAnio = %s", (fecha.year,))
                id_anio_result = dest_cursor.fetchone()
                if not id_anio_result:
                    continue
                
                # Insertar fecha
                dest_cursor.execute(
                    """INSERT IGNORE INTO dim_tiempo 
                       (Fecha, ID_Dia, ID_Mes, ID_Anio) 
                       VALUES (%s, %s, %s, %s)""",
                    (fecha, id_dia_result[0], id_mes_result[0], id_anio_result[0])
                )
                fechas_insertadas += 1
            
            dest_cursor.close()
            self.dest_conn.commit()
            self.stats['dim_tiempo'] = fechas_insertadas
            print(f"   ✅ Insertadas {fechas_insertadas} fechas únicas")
                
        except Exception as e:
            print(f"   ❌ Error cargando dim_tiempo: {e}")
            self.dest_conn.rollback()
    
    def load_dim_proyectos_fixed(self):
        """Carga dim_proyectos corregido."""
        print("\n📊 Cargando dim_proyectos...")
        
        try:
            source_cursor = self.source_conn.cursor()
            dest_cursor = self.dest_conn.cursor()
            
            # Obtener mapeo de clientes del DW
            dest_cursor.execute("SELECT ID_Cliente, CodigoClienteReal FROM dim_clientes")
            mapeo_clientes = {row[1]: row[0] for row in dest_cursor.fetchall()}
            
            # Extraer proyectos de origen
            source_cursor.execute("""
                SELECT DISTINCT 
                    p.ID_Proyecto,
                    p.Estado,
                    p.CostoPresupuestado,
                    p.CostoReal,
                    cli.ID_Cliente as ClienteOrigen
                FROM proyectos p
                INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
                INNER JOIN clientes cli ON c.ID_Cliente = cli.ID_Cliente
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                AND c.Estado IN ('Cerrado', 'Cancelado')
            """)
            
            proyectos = source_cursor.fetchall()
            source_cursor.close()
            
            # Insertar proyectos
            proyectos_insertados = 0
            for proyecto in proyectos:
                cliente_dw_id = mapeo_clientes.get(proyecto[4])
                if cliente_dw_id:
                    dest_cursor.execute(
                        """INSERT IGNORE INTO dim_proyectos 
                           (CodigoProyecto, Estado, CostoPresupuestado, CostoReal, ID_Cliente) 
                           VALUES (%s, %s, %s, %s, %s)""",
                        (str(proyecto[0]), proyecto[1], proyecto[2], proyecto[3], cliente_dw_id)
                    )
                    proyectos_insertados += 1
            
            dest_cursor.close()
            self.dest_conn.commit()
            self.stats['dim_proyectos'] = proyectos_insertados
            print(f"   ✅ Insertados {proyectos_insertados} proyectos")
                
        except Exception as e:
            print(f"   ❌ Error cargando dim_proyectos: {e}")
            self.dest_conn.rollback()
    
    def load_dim_tareas_fixed(self):
        """Carga dim_tareas corregido."""
        print("\n📝 Cargando dim_tareas...")
        
        try:
            source_cursor = self.source_conn.cursor()
            dest_cursor = self.dest_conn.cursor()
            
            # Obtener mapeo de proyectos del DW
            dest_cursor.execute("SELECT ID_proyectos, CodigoProyecto FROM dim_proyectos")
            mapeo_proyectos = {int(row[1]): row[0] for row in dest_cursor.fetchall()}
            
            # Extraer tareas de origen
            source_cursor.execute("""
                SELECT DISTINCT 
                    t.ID_Tarea,
                    t.DuracionPlanificada,
                    t.DuracionReal,
                    t.ID_Proyecto
                FROM tareas t
                INNER JOIN proyectos p ON t.ID_Proyecto = p.ID_Proyecto
                INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                AND c.Estado IN ('Cerrado', 'Cancelado')
            """)
            
            tareas = source_cursor.fetchall()
            source_cursor.close()
            
            # Insertar tareas
            tareas_insertadas = 0
            for tarea in tareas:
                proyecto_dw_id = mapeo_proyectos.get(tarea[3])
                if proyecto_dw_id:
                    dest_cursor.execute(
                        """INSERT IGNORE INTO dim_tareas 
                           (CodigoTarea, DuracionPlanificadaWeek, DuracionRealWeek, dim_proyectos_ID_proyectos) 
                           VALUES (%s, %s, %s, %s)""",
                        (tarea[0], tarea[1], tarea[2], proyecto_dw_id)
                    )
                    tareas_insertadas += 1
            
            dest_cursor.close()
            self.dest_conn.commit()
            self.stats['dim_tareas'] = tareas_insertadas
            print(f"   ✅ Insertadas {tareas_insertadas} tareas")
                
        except Exception as e:
            print(f"   ❌ Error cargando dim_tareas: {e}")
            self.dest_conn.rollback()
    
    def run_fixed_etl(self):
        """Ejecuta ETL corregido."""
        print("🚀 EJECUTANDO ETL CORREGIDO")
        print("=" * 40)
        
        if not self.connect():
            return False
        
        try:
            self.load_dim_tiempo_fixed()
            self.load_dim_proyectos_fixed()
            self.load_dim_tareas_fixed()
            
            print(f"\n📊 RESUMEN:")
            for tabla, count in self.stats.items():
                print(f"   📋 {tabla}: {count:,} registros")
            
            print(f"\n🎉 ¡ETL corregido completado!")
            return True
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
        finally:
            self.disconnect()

def main():
    etl = ETLProcessorFixed()
    etl.run_fixed_etl()

if __name__ == "__main__":
    main()