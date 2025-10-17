#!/usr/bin/env python3
"""
Script para poblar los catálogos del Data Warehouse antes del proceso ETL principal
"""

import sys
import os
sys.path.append('scripts')

from src.db_connector import get_db_connection

def populate_catalogs():
    """Puebla los catálogos base del Data Warehouse."""
    print("🏗️ POBLANDO CATÁLOGOS DEL DATA WAREHOUSE")
    print("=" * 50)
    
    # Obtener conexiones
    source_conn = get_db_connection('origen')
    dest_conn = get_db_connection('destino')
    
    if not source_conn or not dest_conn:
        print("❌ Error: No se pudieron establecer las conexiones")
        return False
    
    try:
        with source_conn.cursor() as source_cursor, dest_conn.cursor() as dest_cursor:
            
            # 1. POBLAR dim_tipo_riesgo
            print("\n1. 📋 Poblando dim_tipo_riesgo...")
            try:
                source_cursor.execute("SELECT DISTINCT TipoRiesgo FROM riesgos WHERE TipoRiesgo IS NOT NULL")
                tipos_riesgo = source_cursor.fetchall()
                
                if tipos_riesgo:
                    for tipo in tipos_riesgo:
                        dest_cursor.execute(
                            "INSERT IGNORE INTO dim_tipo_riesgo (NombreTipo) VALUES (%s)",
                            (tipo[0],)
                        )
                    print(f"   ✅ Insertados {len(tipos_riesgo)} tipos de riesgo")
                else:
                    print("   ⚠️ No se encontraron tipos de riesgo en origen")
            except Exception as e:
                print(f"   ❌ Error poblando tipos de riesgo: {e}")
            
            # 2. POBLAR dim_severidad 
            print("\n2. 📋 Poblando dim_severidad...")
            try:
                source_cursor.execute("SELECT DISTINCT Severidad FROM riesgos WHERE Severidad IS NOT NULL")
                severidades = source_cursor.fetchall()
                
                if severidades:
                    for sev in severidades:
                        dest_cursor.execute(
                            "INSERT IGNORE INTO dim_severidad (Nivel) VALUES (%s)",
                            (sev[0],)
                        )
                    print(f"   ✅ Insertadas {len(severidades)} severidades")
                else:
                    print("   ⚠️ No se encontraron severidades en origen")
            except Exception as e:
                print(f"   ❌ Error poblando severidades: {e}")
            
            # 3. POBLAR subdimensiones de tiempo
            print("\n3. 📅 Poblando dimensiones de tiempo...")
            
            # Años (desde 2020 hasta 2030)
            print("   📅 Insertando años...")
            for year in range(2020, 2031):
                dest_cursor.execute(
                    "INSERT IGNORE INTO subdim_anio (NumeroAnio) VALUES (%s)",
                    (year,)
                )
            
            # Meses (1-12)
            print("   📅 Insertando meses...")
            for month in range(1, 13):
                dest_cursor.execute(
                    "INSERT IGNORE INTO subdim_mes (NumeroMes) VALUES (%s)",
                    (month,)
                )
            
            # Días de semana (1-7, Lunes=1)
            print("   📅 Insertando días de semana...")
            for day in range(1, 8):
                dest_cursor.execute(
                    "INSERT IGNORE INTO subdim_dia (NumeroDiaSemana) VALUES (%s)",
                    (day,)
                )
            
            print("   ✅ Dimensiones de tiempo completadas")
            
            # 4. VERIFICAR ESTADOS DE PROYECTOS
            print("\n4. 📊 Analizando estados de proyectos...")
            source_cursor.execute("SELECT DISTINCT Estado FROM proyectos WHERE Estado IS NOT NULL")
            estados_proyecto = [estado[0] for estado in source_cursor.fetchall()]
            print(f"   📋 Estados encontrados: {estados_proyecto}")
            
            # 5. VERIFICAR TIPOS DE GASTO
            print("\n5. 💰 Analizando tipos de gasto...")
            try:
                source_cursor.execute("SELECT DISTINCT 'Gasto Directo' as tipo UNION SELECT DISTINCT 'Penalizacion' as tipo")
                tipos_gasto = [tipo[0] for tipo in source_cursor.fetchall()]
                print(f"   📋 Tipos de gasto sugeridos: {tipos_gasto}")
                
                # Insertar tipos básicos de finanzas
                for tipo in ['Gasto Directo', 'Penalizacion', 'Costo Horas']:
                    dest_cursor.execute(
                        "INSERT IGNORE INTO dim_finanzas (TipoGasto, Monto) VALUES (%s, %s)",
                        (tipo, 0.0)
                    )
                print("   ✅ Tipos básicos de finanzas insertados")
            except Exception as e:
                print(f"   ❌ Error analizando gastos: {e}")
            
            # Confirmar cambios
            dest_conn.commit()
            print(f"\n🎉 ¡Catálogos poblados exitosamente!")
            
            # Mostrar resumen
            print(f"\n📊 RESUMEN DE CATÁLOGOS POBLADOS:")
            
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
    success = populate_catalogs()
    if success:
        print(f"\n🚀 Catálogos listos para el proceso ETL principal!")
    else:
        print(f"\n❌ Hubo errores poblando los catálogos.")