#!/usr/bin/env python3
"""
Analizador de estructura de tablas origen para validar el mapeo ETL
"""

import sys
import os
sys.path.append('scripts')

from src.db_connector import get_db_connection

def analyze_source_tables():
    """Analiza la estructura de las tablas origen relevantes para el ETL."""
    print("🔍 ANÁLISIS DE ESTRUCTURA - TABLAS ORIGEN")
    print("=" * 60)
    
    conn = get_db_connection('origen')
    if not conn:
        print("❌ No se pudo conectar a la base de datos origen")
        return False
    
    # Tablas críticas para el mapeo
    critical_tables = [
        'riesgos', 'gastos', 'hitos', 'empleados', 
        'asignaciones', 'errores', 'pruebas', 'proyectos'
    ]
    
    try:
        with conn.cursor() as cursor:
            
            for table_name in critical_tables:
                print(f"\n📋 TABLA: {table_name}")
                print("=" * 40)
                
                try:
                    # Estructura de la tabla
                    cursor.execute(f"DESCRIBE {table_name}")
                    columns = cursor.fetchall()
                    
                    print("   Columnas:")
                    for col in columns:
                        field, type_info, null, key, default, extra = col
                        key_info = f" [{key}]" if key else ""
                        print(f"     • {field}: {type_info}{key_info}")
                    
                    # Contar registros
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    print(f"   📊 Registros: {count:,}")
                    
                    # Análisis específico según tabla
                    if table_name == 'riesgos' and count > 0:
                        print("   🔍 Análisis de contenido:")
                        cursor.execute("SELECT DISTINCT * FROM riesgos LIMIT 3")
                        samples = cursor.fetchall()
                        for i, row in enumerate(samples, 1):
                            print(f"     {i}. {row}")
                    
                    elif table_name == 'gastos' and count > 0:
                        print("   🔍 Tipos de gasto únicos:")
                        try:
                            cursor.execute("SELECT DISTINCT TipoGasto FROM gastos LIMIT 5")
                            tipos = cursor.fetchall()
                            for tipo in tipos:
                                print(f"     • {tipo[0]}")
                        except:
                            cursor.execute("SELECT * FROM gastos LIMIT 3")
                            samples = cursor.fetchall()
                            print("     Datos de ejemplo:")
                            for row in samples:
                                print(f"     {row}")
                    
                    elif table_name == 'empleados' and count > 0:
                        print("   🔍 Verificando campos de cálculo:")
                        cursor.execute("SELECT * FROM empleados LIMIT 2")
                        samples = cursor.fetchall()
                        for row in samples:
                            print(f"     {row}")
                    
                    elif table_name == 'hitos' and count > 0:
                        print("   🔍 Campos de fecha para cálculo de retraso:")
                        cursor.execute("SELECT * FROM hitos LIMIT 2")
                        samples = cursor.fetchall()
                        for row in samples:
                            print(f"     {row}")
                            
                except Exception as e:
                    print(f"   ❌ Error analizando {table_name}: {e}")
                
                print("-" * 40)
            
            return True
            
    except Exception as e:
        print(f"❌ Error general: {e}")
        return False
        
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("\n✅ Análisis completado. Conexión cerrada.")

if __name__ == "__main__":
    success = analyze_source_tables()
    if success:
        print(f"\n🎯 Análisis completado. Revisa la estructura para confirmar el mapeo.")
    else:
        print(f"\n❌ Hubo problemas durante el análisis.")