#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Validator - Módulo para validar el estado de los catálogos y dimensiones
"""

import mysql.connector
from pathlib import Path
from dotenv import load_dotenv
import os

class ETLValidator:
    """Clase para validar el estado del data warehouse"""
    
    def __init__(self):
        # Cargar variables de entorno
        project_root = Path(__file__).parent.parent.parent
        load_dotenv(project_root / 'config' / '.env')
        
        self.config_destino = {
            'host': os.getenv('DESTINO_HOST'),
            'user': os.getenv('DESTINO_USER'),
            'password': os.getenv('DESTINO_PASS'),
            'database': os.getenv('DESTINO_DB'),
            'charset': 'utf8mb4',
            'buffered': True
        }
        
        # Definir catálogos y dimensiones requeridas
        self.catalogos_requeridos = {
            'dim_tipo_riesgo': 800,      # Mínimo esperado
            'dim_severidad': 3,          # Exacto
            'subdim_anio': 10,           # Mínimo esperado
            'subdim_mes': 12,            # Exacto
            'subdim_dia': 7              # Exacto
        }
        
        self.dimensiones_principales = [
            'dim_clientes', 'dim_empleados', 'dim_proyectos', 
            'dim_tareas', 'dim_tiempo', 'dim_finanzas'
        ]
    
    def validate_catalogs(self):
        """Valida si los catálogos están poblados correctamente"""
        print("🔍 Validando catálogos del data warehouse...")
        
        try:
            conn = mysql.connector.connect(**self.config_destino)
            cursor = conn.cursor(buffered=True)
            
            missing_catalogs = []
            populated_catalogs = []
            
            for catalogo, min_records in self.catalogos_requeridos.items():
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {catalogo}")
                    count = cursor.fetchone()[0]
                    
                    if count >= min_records:
                        populated_catalogs.append((catalogo, count))
                        print(f"✅ {catalogo}: {count} registros (≥{min_records})")
                    else:
                        missing_catalogs.append((catalogo, count, min_records))
                        print(f"❌ {catalogo}: {count} registros (<{min_records})")
                        
                except mysql.connector.Error as e:
                    if "doesn't exist" in str(e):
                        missing_catalogs.append((catalogo, 0, min_records))
                        print(f"❌ {catalogo}: Tabla no existe")
                    else:
                        print(f"❌ Error verificando {catalogo}: {e}")
            
            cursor.close()
            conn.close()
            
            return len(missing_catalogs) == 0, missing_catalogs, populated_catalogs
            
        except Exception as e:
            print(f"❌ Error conectando a la base de datos: {e}")
            return False, [], []
    
    def validate_dimensions(self):
        """Valida si las dimensiones principales están pobladas"""
        print("\n🔍 Validando dimensiones principales...")
        
        try:
            conn = mysql.connector.connect(**self.config_destino)
            cursor = conn.cursor(buffered=True)
            
            dimension_status = {}
            
            for dimension in self.dimensiones_principales:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {dimension}")
                    count = cursor.fetchone()[0]
                    dimension_status[dimension] = count
                    
                    status = "✅" if count > 0 else "❌"
                    print(f"{status} {dimension}: {count} registros")
                    
                except mysql.connector.Error as e:
                    if "doesn't exist" in str(e):
                        dimension_status[dimension] = 0
                        print(f"❌ {dimension}: Tabla no existe")
                    else:
                        print(f"❌ Error verificando {dimension}: {e}")
                        dimension_status[dimension] = -1
            
            cursor.close()
            conn.close()
            
            # Determinar si las dimensiones están listas
            dimensions_ready = all(count > 0 for count in dimension_status.values())
            
            return dimensions_ready, dimension_status
            
        except Exception as e:
            print(f"❌ Error conectando a la base de datos: {e}")
            return False, {}
    
    def validate_fact_table(self):
        """Valida si la tabla de hechos está poblada"""
        print("\n🔍 Validando tabla de hechos...")
        
        try:
            conn = mysql.connector.connect(**self.config_destino)
            cursor = conn.cursor(buffered=True)
            
            try:
                cursor.execute("SELECT COUNT(*) FROM hechos_proyectos")
                count = cursor.fetchone()[0]
                
                if count > 0:
                    # Verificar calidad de los datos
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total,
                            COUNT(CASE WHEN Ganancias > 0 THEN 1 END) as con_ganancias,
                            AVG(Ganancias) as ganancia_promedio
                        FROM hechos_proyectos
                    """)
                    stats = cursor.fetchone()
                    
                    print(f"✅ hechos_proyectos: {count} registros")
                    print(f"   📈 Con ganancias: {stats[1]}")
                    print(f"   💰 Ganancia promedio: ${stats[2]:.2f}")
                    
                    fact_ready = True
                else:
                    print(f"❌ hechos_proyectos: {count} registros")
                    fact_ready = False
                    
            except mysql.connector.Error as e:
                if "doesn't exist" in str(e):
                    print("❌ hechos_proyectos: Tabla no existe")
                    count = 0
                else:
                    print(f"❌ Error verificando hechos_proyectos: {e}")
                    count = -1
                fact_ready = False
            
            cursor.close()
            conn.close()
            
            return fact_ready, count
            
        except Exception as e:
            print(f"❌ Error conectando a la base de datos: {e}")
            return False, 0
    
    def get_full_status(self):
        """Obtiene el estado completo del data warehouse"""
        print("📊 VALIDACIÓN COMPLETA DEL DATA WAREHOUSE")
        print("=" * 50)
        
        # Validar catálogos
        catalogs_ready, missing_catalogs, populated_catalogs = self.validate_catalogs()
        
        # Validar dimensiones
        dimensions_ready, dimension_status = self.validate_dimensions()
        
        # Validar tabla de hechos
        fact_ready, fact_count = self.validate_fact_table()
        
        # Resumen
        print(f"\n📋 RESUMEN DE VALIDACIÓN:")
        print(f"   📚 Catálogos: {'✅ Listos' if catalogs_ready else '❌ Incompletos'}")
        print(f"   📊 Dimensiones: {'✅ Listos' if dimensions_ready else '❌ Incompletas'}")
        print(f"   🎯 Hechos: {'✅ Listo' if fact_ready else '❌ Incompleto'}")
        
        etl_ready = catalogs_ready and dimensions_ready and fact_ready
        print(f"\n🎯 Estado ETL: {'✅ COMPLETO' if etl_ready else '❌ REQUIERE PROCESAMIENTO'}")
        
        return {
            'etl_ready': etl_ready,
            'catalogs_ready': catalogs_ready,
            'dimensions_ready': dimensions_ready,
            'fact_ready': fact_ready,
            'missing_catalogs': missing_catalogs,
            'dimension_status': dimension_status,
            'fact_count': fact_count
        }

if __name__ == "__main__":
    validator = ETLValidator()
    status = validator.get_full_status()