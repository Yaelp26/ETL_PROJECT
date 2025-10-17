#!/usr/bin/env python3
"""
Completar la población de catálogos con datos reales de las tablas origen
"""

import sys
import os
sys.path.append('scripts')

from src.db_connector import get_db_connection

def complete_catalogs():
    """Completa los catálogos con datos reales de las tablas origen."""
    print("🏗️ COMPLETANDO CATÁLOGOS CON DATOS REALES")
    print("=" * 50)
    
    source_conn = get_db_connection('origen')
    dest_conn = get_db_connection('destino')
    
    if not source_conn or not dest_conn:
        print("❌ Error: No se pudieron establecer las conexiones")
        return False
    
    try:
        with source_conn.cursor() as source_cursor, dest_conn.cursor() as dest_cursor:
            
            # 1. COMPLETAR dim_tipo_riesgo (corregido)
            print("\n1. 📋 Poblando dim_tipo_riesgo...")
            try:
                source_cursor.execute("SELECT DISTINCT Tipo FROM riesgos WHERE Tipo IS NOT NULL")
                tipos_riesgo = source_cursor.fetchall()
                
                if tipos_riesgo:
                    for tipo in tipos_riesgo:
                        dest_cursor.execute(
                            "INSERT IGNORE INTO dim_tipo_riesgo (NombreTipo) VALUES (%s)",
                            (tipo[0],)
                        )
                    dest_conn.commit()
                    print(f"   ✅ Insertados {len(tipos_riesgo)} tipos de riesgo")
                    
                    # Mostrar tipos insertados
                    for tipo in tipos_riesgo:
                        print(f"     • {tipo[0]}")
                else:
                    print("   ⚠️ No se encontraron tipos de riesgo")
            except Exception as e:
                print(f"   ❌ Error poblando tipos de riesgo: {e}")
            
            # 2. COMPLETAR dim_finanzas con tipos de gasto reales
            print("\n2. 💰 Poblando dim_finanzas con tipos de gasto...")
            try:
                source_cursor.execute("SELECT DISTINCT TipoGasto FROM gastos WHERE TipoGasto IS NOT NULL")
                tipos_gasto = source_cursor.fetchall()
                
                if tipos_gasto:
                    for tipo in tipos_gasto:
                        dest_cursor.execute(
                            "INSERT IGNORE INTO dim_finanzas (TipoGasto, Monto) VALUES (%s, %s)",
                            (tipo[0], 0.0)
                        )
                    
                    # Agregar tipo para penalizaciones si no existe
                    dest_cursor.execute(
                        "INSERT IGNORE INTO dim_finanzas (TipoGasto, Monto) VALUES (%s, %s)",
                        ('Penalizacion', 0.0)
                    )
                    
                    dest_conn.commit()
                    print(f"   ✅ Insertados {len(tipos_gasto)} tipos de gasto + Penalizacion")
                    
                    # Mostrar tipos insertados
                    for tipo in tipos_gasto:
                        print(f"     • {tipo[0]}")
                    print(f"     • Penalizacion")
                else:
                    print("   ⚠️ No se encontraron tipos de gasto")
            except Exception as e:
                print(f"   ❌ Error poblando tipos de gasto: {e}")
            
            # 3. VERIFICAR ESTADO FINAL DE CATÁLOGOS
            print(f"\n3. 📊 RESUMEN FINAL DE CATÁLOGOS:")
            
            catalogs = [
                ("dim_tipo_riesgo", "NombreTipo"),
                ("dim_severidad", "Nivel"), 
                ("subdim_anio", "NumeroAnio"),
                ("subdim_mes", "NumeroMes"),
                ("subdim_dia", "NumeroDiaSemana"),
                ("dim_finanzas", "TipoGasto")
            ]
            
            for table, field in catalogs:
                dest_cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = dest_cursor.fetchone()[0]
                print(f"   📋 {table}: {count} registros")
                
                # Mostrar algunos valores para dim_tipo_riesgo y dim_finanzas
                if table == "dim_tipo_riesgo" and count > 0:
                    dest_cursor.execute(f"SELECT {field} FROM {table} LIMIT 5")
                    values = dest_cursor.fetchall()
                    print(f"     Valores: {[v[0] for v in values]}")
                    
                elif table == "dim_finanzas" and count > 0:
                    dest_cursor.execute(f"SELECT DISTINCT {field} FROM {table} LIMIT 7")
                    values = dest_cursor.fetchall()
                    print(f"     Tipos: {[v[0] for v in values]}")
            
            print(f"\n🎉 ¡Catálogos completados exitosamente!")
            return True
            
    except Exception as e:
        print(f"❌ Error general: {e}")
        if dest_conn:
            dest_conn.rollback()
        return False
        
    finally:
        if source_conn and source_conn.is_connected():
            source_conn.close()
        if dest_conn and dest_conn.is_connected():
            dest_conn.close()
        print(f"\n✅ Conexiones cerradas.")

if __name__ == "__main__":
    success = complete_catalogs()
    if success:
        print(f"\n🚀 Catálogos completos. Listos para ETL principal!")
    else:
        print(f"\n❌ Hubo errores completando los catálogos.")