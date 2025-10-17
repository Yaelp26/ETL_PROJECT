#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Catalog Loader - Módulo para poblar catálogos y subdimensiones
"""

import mysql.connector
from pathlib import Path
from dotenv import load_dotenv
import os
import calendar

class ETLCatalogLoader:
    """Clase para poblar catálogos del data warehouse"""
    
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
    
    def load_subdim_tiempo(self):
        """Carga las subdimensiones de tiempo (año, mes, día)"""
        print("📅 Cargando subdimensiones de tiempo...")
        
        try:
            conn_origen = mysql.connector.connect(**self.config_origen)
            conn_destino = mysql.connector.connect(**self.config_destino)
            
            cursor_origen = conn_origen.cursor(buffered=True)
            cursor_destino = conn_destino.cursor(buffered=True)
            
            # 1. Cargar subdim_anio
            print("   📅 Cargando subdim_anio...")
            cursor_origen.execute("""
                SELECT DISTINCT YEAR(p.FechaInicio) as anio
                FROM proyectos p
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                UNION
                SELECT DISTINCT YEAR(p.FechaFin) as anio
                FROM proyectos p
                WHERE p.Estado IN ('Cancelado', 'Cerrado') AND p.FechaFin IS NOT NULL
                ORDER BY anio
            """)
            años = [row[0] for row in cursor_origen.fetchall()]
            
            for año in años:
                cursor_destino.execute("""
                    INSERT IGNORE INTO subdim_anio (ID_Anio, NumeroAnio)
                    VALUES (%s, %s)
                """, (año, año))
            
            # 2. Cargar subdim_mes
            print("   📅 Cargando subdim_mes...")
            meses = [(i, i) for i in range(1, 13)]
            cursor_destino.executemany("""
                INSERT IGNORE INTO subdim_mes (ID_Mes, NumeroMes)
                VALUES (%s, %s)
            """, meses)
            
            # 3. Cargar subdim_dia
            print("   📅 Cargando subdim_dia...")
            dias = [(i, i) for i in range(1, 8)]  # 1=Lunes, 7=Domingo
            cursor_destino.executemany("""
                INSERT IGNORE INTO subdim_dia (ID_Dia, NumeroDiaSemana)
                VALUES (%s, %s)
            """, dias)
            
            conn_destino.commit()
            
            # Verificar resultados
            cursor_destino.execute("SELECT COUNT(*) FROM subdim_anio")
            count_anio = cursor_destino.fetchone()[0]
            cursor_destino.execute("SELECT COUNT(*) FROM subdim_mes")
            count_mes = cursor_destino.fetchone()[0]
            cursor_destino.execute("SELECT COUNT(*) FROM subdim_dia")
            count_dia = cursor_destino.fetchone()[0]
            
            print(f"   ✅ subdim_anio: {count_anio} registros")
            print(f"   ✅ subdim_mes: {count_mes} registros")
            print(f"   ✅ subdim_dia: {count_dia} registros")
            
            cursor_origen.close()
            cursor_destino.close()
            conn_origen.close()
            conn_destino.close()
            
            return True
            
        except Exception as e:
            print(f"❌ Error cargando subdimensiones tiempo: {e}")
            return False
    
    def load_dim_tipo_riesgo(self):
        """Carga la dimensión de tipos de riesgo"""
        print("⚠️ Cargando dim_tipo_riesgo...")
        
        try:
            conn_origen = mysql.connector.connect(**self.config_origen)
            conn_destino = mysql.connector.connect(**self.config_destino)
            
            cursor_origen = conn_origen.cursor(buffered=True)
            cursor_destino = conn_destino.cursor(buffered=True)
            
            # Extraer tipos de riesgo de la fuente
            cursor_origen.execute("""
                SELECT DISTINCT 
                    r.ID_Riesgo, 
                    r.Descripcion,
                    r.Categoria,
                    COALESCE(r.Probabilidad, 0) as Probabilidad,
                    COALESCE(r.Impacto, 0) as Impacto
                FROM riesgos r
                INNER JOIN proyectos p ON r.ID_Proyecto = p.ID_Proyecto
                WHERE p.Estado IN ('Cancelado', 'Cerrado')
                ORDER BY r.ID_Riesgo
            """)
            
            riesgos = cursor_origen.fetchall()
            
            if riesgos:
                # Insertar en dim_tipo_riesgo
                query_insert = """
                    INSERT IGNORE INTO dim_tipo_riesgo 
                    (ID_TipoRiesgo, Descripcion, Categoria, Probabilidad, Impacto)
                    VALUES (%s, %s, %s, %s, %s)
                """
                
                cursor_destino.executemany(query_insert, riesgos)
                conn_destino.commit()
                
                # Verificar resultado
                cursor_destino.execute("SELECT COUNT(*) FROM dim_tipo_riesgo")
                count = cursor_destino.fetchone()[0]
                print(f"   ✅ dim_tipo_riesgo: {count} registros")
            else:
                print("   ⚠️ No se encontraron tipos de riesgo")
            
            cursor_origen.close()
            cursor_destino.close()
            conn_origen.close()
            conn_destino.close()
            
            return True
            
        except Exception as e:
            print(f"❌ Error cargando dim_tipo_riesgo: {e}")
            return False
    
    def load_dim_severidad(self):
        """Carga la dimensión de severidad de riesgo"""
        print("📊 Cargando dim_severidad...")
        
        try:
            conn_destino = mysql.connector.connect(**self.config_destino)
            cursor_destino = conn_destino.cursor(buffered=True)
            
            # Datos predefinidos de severidad
            severidades = [
                (1, 'Baja', 'Impacto mínimo en el proyecto'),
                (2, 'Media', 'Impacto moderado en el proyecto'),
                (3, 'Alta', 'Impacto significativo en el proyecto')
            ]
            
            query_insert = """
                INSERT IGNORE INTO dim_severidad 
                (ID_Severidad, Nivel, Descripcion)
                VALUES (%s, %s, %s)
            """
            
            cursor_destino.executemany(query_insert, severidades)
            conn_destino.commit()
            
            # Verificar resultado
            cursor_destino.execute("SELECT COUNT(*) FROM dim_severidad")
            count = cursor_destino.fetchone()[0]
            print(f"   ✅ dim_severidad: {count} registros")
            
            cursor_destino.close()
            conn_destino.close()
            
            return True
            
        except Exception as e:
            print(f"❌ Error cargando dim_severidad: {e}")
            return False
    
    def load_all_catalogs(self):
        """Carga todos los catálogos necesarios"""
        print("📚 CARGANDO TODOS LOS CATÁLOGOS")
        print("=" * 40)
        
        success_count = 0
        total_catalogs = 3
        
        # Cargar subdimensiones tiempo
        if self.load_subdim_tiempo():
            success_count += 1
        
        # Cargar tipos de riesgo
        if self.load_dim_tipo_riesgo():
            success_count += 1
        
        # Cargar severidades
        if self.load_dim_severidad():
            success_count += 1
        
        print(f"\n📊 Resultado: {success_count}/{total_catalogs} catálogos cargados")
        
        if success_count == total_catalogs:
            print("✅ ¡Todos los catálogos fueron cargados exitosamente!")
            return True
        else:
            print("❌ Algunos catálogos no pudieron ser cargados")
            return False

if __name__ == "__main__":
    loader = ETLCatalogLoader()
    loader.load_all_catalogs()