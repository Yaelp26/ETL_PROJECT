#!/usr/bin/env python3
"""
Corrección específica para dim_tiempo
"""

import sys
import os
sys.path.append('scripts')

from src.db_connector import get_db_connection

def fix_dim_tiempo():
    """Corrige la carga de dim_tiempo."""
    print("📅 CORRIGIENDO DIMENSIÓN TIEMPO")
    print("=" * 40)
    
    source_conn = get_db_connection('origen')
    dest_conn = get_db_connection('destino')
    
    if not source_conn or not dest_conn:
        print("❌ Error: No se pudieron establecer las conexiones")
        return False
    
    try:
        # 1. Verificar estado actual
        dest_cursor = dest_conn.cursor()
        dest_cursor.execute("SELECT COUNT(*) FROM dim_tiempo")
        count_actual = dest_cursor.fetchone()[0]
        print(f"📊 Registros actuales en dim_tiempo: {count_actual}")
        dest_cursor.close()
        
        # 2. Obtener años únicos de proyectos filtrados
        print("\n🔍 Analizando fechas de proyectos...")
        source_cursor = source_conn.cursor()
        
        source_cursor.execute("""
            SELECT DISTINCT YEAR(FechaInicio) as anio 
            FROM proyectos p
            INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
            WHERE p.Estado IN ('Cancelado', 'Cerrado') 
            AND c.Estado IN ('Cerrado', 'Cancelado')
            AND FechaInicio IS NOT NULL
            UNION
            SELECT DISTINCT YEAR(FechaFin) as anio 
            FROM proyectos p
            INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
            WHERE p.Estado IN ('Cancelado', 'Cerrado') 
            AND c.Estado IN ('Cerrado', 'Cancelado')
            AND FechaFin IS NOT NULL
            ORDER BY anio
        """)
        
        años_necesarios = [row[0] for row in source_cursor.fetchall()]
        print(f"   📋 Años encontrados: {años_necesarios}")
        
        # 3. Asegurar que tenemos todos los años en subdim_anio
        dest_cursor = dest_conn.cursor()
        for año in años_necesarios:
            dest_cursor.execute(
                "INSERT IGNORE INTO subdim_anio (NumeroAnio) VALUES (%s)",
                (año,)
            )
        dest_conn.commit()
        print(f"   ✅ Años insertados en subdim_anio")
        
        # 4. Obtener fechas únicas (sin UNION en una sola consulta)
        print("\n📅 Extrayendo fechas únicas...")
        
        # Fechas de inicio
        source_cursor.execute("""
            SELECT DISTINCT FechaInicio as Fecha 
            FROM proyectos p
            INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
            WHERE p.Estado IN ('Cancelado', 'Cerrado')
            AND c.Estado IN ('Cerrado', 'Cancelado')
            AND FechaInicio IS NOT NULL
        """)
        fechas_inicio = [row[0] for row in source_cursor.fetchall()]
        
        # Fechas de fin
        source_cursor.execute("""
            SELECT DISTINCT FechaFin as Fecha 
            FROM proyectos p
            INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
            WHERE p.Estado IN ('Cancelado', 'Cerrado')
            AND c.Estado IN ('Cerrado', 'Cancelado')
            AND FechaFin IS NOT NULL
        """)
        fechas_fin = [row[0] for row in source_cursor.fetchall()]
        
        # Combinar y eliminar duplicados
        todas_las_fechas = list(set(fechas_inicio + fechas_fin))
        todas_las_fechas.sort()
        
        print(f"   📊 Total fechas únicas encontradas: {len(todas_las_fechas)}")
        print(f"   📅 Rango: {min(todas_las_fechas)} a {max(todas_las_fechas)}")
        
        source_cursor.close()
        
        # 5. Procesar cada fecha
        print(f"\n⏳ Procesando fechas...")
        fechas_insertadas = 0
        errores = 0
        
        for fecha in todas_las_fechas:
            try:
                # Obtener día de la semana (1=Lunes, 7=Domingo)
                dia_semana = fecha.isoweekday()
                
                # Buscar IDs en subdimensiones
                dest_cursor.execute("SELECT ID_Dia FROM subdim_dia WHERE NumeroDiaSemana = %s", (dia_semana,))
                result_dia = dest_cursor.fetchone()
                if not result_dia:
                    print(f"   ⚠️  No se encontró día {dia_semana} para fecha {fecha}")
                    errores += 1
                    continue
                id_dia = result_dia[0]
                
                dest_cursor.execute("SELECT ID_Mes FROM subdim_mes WHERE NumeroMes = %s", (fecha.month,))
                result_mes = dest_cursor.fetchone()
                if not result_mes:
                    print(f"   ⚠️  No se encontró mes {fecha.month} para fecha {fecha}")
                    errores += 1
                    continue
                id_mes = result_mes[0]
                
                dest_cursor.execute("SELECT ID_Anio FROM subdim_anio WHERE NumeroAnio = %s", (fecha.year,))
                result_anio = dest_cursor.fetchone()
                if not result_anio:
                    print(f"   ⚠️  No se encontró año {fecha.year} para fecha {fecha}")
                    errores += 1
                    continue
                id_anio = result_anio[0]
                
                # Insertar fecha en dim_tiempo
                dest_cursor.execute(
                    """INSERT IGNORE INTO dim_tiempo 
                       (Fecha, ID_Dia, ID_Mes, ID_Anio) 
                       VALUES (%s, %s, %s, %s)""",
                    (fecha, id_dia, id_mes, id_anio)
                )
                fechas_insertadas += 1
                
            except Exception as e:
                print(f"   ❌ Error procesando fecha {fecha}: {e}")
                errores += 1
        
        dest_cursor.close()
        dest_conn.commit()
        
        # 6. Verificar resultado final
        dest_cursor = dest_conn.cursor()
        dest_cursor.execute("SELECT COUNT(*) FROM dim_tiempo")
        count_final = dest_cursor.fetchone()[0]
        
        print(f"\n📊 RESULTADOS:")
        print(f"   📅 Fechas procesadas: {len(todas_las_fechas)}")
        print(f"   ✅ Fechas insertadas exitosamente: {fechas_insertadas}")
        print(f"   ❌ Errores: {errores}")
        print(f"   📋 Total registros en dim_tiempo: {count_final}")
        
        # Mostrar muestra de fechas insertadas
        if count_final > 0:
            dest_cursor.execute("""
                SELECT dt.Fecha, sd.NumeroDiaSemana, sm.NumeroMes, sa.NumeroAnio
                FROM dim_tiempo dt
                JOIN subdim_dia sd ON dt.ID_Dia = sd.ID_Dia
                JOIN subdim_mes sm ON dt.ID_Mes = sm.ID_Mes  
                JOIN subdim_anio sa ON dt.ID_Anio = sa.ID_Anio
                ORDER BY dt.Fecha
                LIMIT 5
            """)
            muestra = dest_cursor.fetchall()
            print(f"\n🔍 MUESTRA DE FECHAS INSERTADAS:")
            for fecha_data in muestra:
                print(f"   📅 {fecha_data[0]} (Día {fecha_data[1]}, Mes {fecha_data[2]}, Año {fecha_data[3]})")
        
        dest_cursor.close()
        
        print(f"\n🎉 ¡Dimensión tiempo corregida!")
        return True
        
    except Exception as e:
        print(f"❌ Error general: {e}")
        dest_conn.rollback()
        return False
        
    finally:
        if source_conn and source_conn.is_connected():
            source_conn.close()
        if dest_conn and dest_conn.is_connected():
            dest_conn.close()
        print("✅ Conexiones cerradas")

def main():
    success = fix_dim_tiempo()
    if success:
        print(f"\n🏆 ¡DIMENSIÓN TIEMPO CORREGIDA EXITOSAMENTE!")
    else:
        print(f"\n❌ Error al corregir dimensión tiempo")

if __name__ == "__main__":
    main()