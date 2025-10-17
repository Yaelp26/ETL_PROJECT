#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector
from pathlib import Path
from dotenv import load_dotenv
import os
from datetime import datetime
import calendar

def fix_dim_tiempo():
    """
    Corrige la dimensión tiempo usando la estructura correcta de las tablas
    """
    print("📅 CORRIGIENDO DIMENSIÓN TIEMPO V3")
    print("=" * 40)
    
    # Cargar variables de entorno
    project_root = Path(__file__).parent
    load_dotenv(project_root / 'config' / '.env')
    
    # Configuraciones de conexión
    config_origen = {
        'host': os.getenv('ORIGEN_HOST'),
        'user': os.getenv('ORIGEN_USER'),
        'password': os.getenv('ORIGEN_PASS'),
        'database': os.getenv('ORIGEN_DB'),
        'charset': 'utf8mb4',
        'buffered': True
    }
    
    config_destino = {
        'host': os.getenv('DESTINO_HOST'),
        'user': os.getenv('DESTINO_USER'),
        'password': os.getenv('DESTINO_PASS'),
        'database': os.getenv('DESTINO_DB'),
        'charset': 'utf8mb4',
        'buffered': True
    }
    
    conn_origen = None
    conn_destino = None
    
    try:
        # Conexiones
        conn_origen = mysql.connector.connect(**config_origen)
        print(" ✅ Conexión exitosa a la base de datos 'origen'.")
        
        conn_destino = mysql.connector.connect(**config_destino)
        print(" ✅ Conexión exitosa a la base de datos 'destino'.")
        
        # Verificar registros actuales
        cursor_destino = conn_destino.cursor(buffered=True)
        cursor_destino.execute("SELECT COUNT(*) FROM dim_tiempo")
        count_actual = cursor_destino.fetchone()[0]
        print(f"📊 Registros actuales en dim_tiempo: {count_actual}")
        cursor_destino.close()
        
        # Limpiar tabla si tiene registros
        if count_actual > 0:
            cursor_destino = conn_destino.cursor(buffered=True)
            cursor_destino.execute("DELETE FROM dim_tiempo")
            conn_destino.commit()
            print("🗑️  Tabla dim_tiempo limpiada")
            cursor_destino.close()
        
        # PASO 1: Verificar que subdim_anio tenga los años
        print("\n🔍 Verificando años en subdim_anio...")
        cursor_origen = conn_origen.cursor(buffered=True)
        query_años = """
        SELECT DISTINCT YEAR(p.FechaInicio) as anio
        FROM proyectos p
        WHERE p.Estado IN ('Cancelado', 'Cerrado')
        UNION
        SELECT DISTINCT YEAR(p.FechaFin) as anio
        FROM proyectos p
        WHERE p.Estado IN ('Cancelado', 'Cerrado') AND p.FechaFin IS NOT NULL
        ORDER BY anio
        """
        cursor_origen.execute(query_años)
        años = [row[0] for row in cursor_origen.fetchall()]
        cursor_origen.close()
        
        print(f"   📋 Años encontrados: {años}")
        
        # Insertar años en subdim_anio usando la estructura correcta
        cursor_destino = conn_destino.cursor(buffered=True)
        for año in años:
            cursor_destino.execute("""
                INSERT IGNORE INTO subdim_anio (ID_Anio, NumeroAnio)
                VALUES (%s, %s)
            """, (año, año))
        conn_destino.commit()
        cursor_destino.close()
        print("   ✅ Años verificados en subdim_anio")
        
        # PASO 2: Extraer todas las fechas únicas
        print("\n📅 Extrayendo fechas únicas...")
        cursor_origen = conn_origen.cursor(buffered=True)
        query_fechas = """
        SELECT DISTINCT DATE(p.FechaInicio) as fecha
        FROM proyectos p
        WHERE p.Estado IN ('Cancelado', 'Cerrado')
        UNION
        SELECT DISTINCT DATE(p.FechaFin) as fecha
        FROM proyectos p
        WHERE p.Estado IN ('Cancelado', 'Cerrado') AND p.FechaFin IS NOT NULL
        ORDER BY fecha
        """
        cursor_origen.execute(query_fechas)
        fechas = [row[0] for row in cursor_origen.fetchall()]
        cursor_origen.close()
        
        print(f"   📊 Total fechas únicas encontradas: {len(fechas)}")
        if fechas:
            print(f"   📅 Rango: {fechas[0]} a {fechas[-1]}")
        
        # PASO 3: Verificar subdimensiones existentes
        print("\n🔍 Verificando subdimensiones...")
        
        # Verificar subdim_mes
        cursor_destino = conn_destino.cursor(buffered=True)
        cursor_destino.execute("SELECT COUNT(*) FROM subdim_mes")
        meses_count = cursor_destino.fetchone()[0]
        cursor_destino.close()
        
        if meses_count == 0:
            print("   📅 Creando subdim_mes...")
            cursor_destino = conn_destino.cursor(buffered=True)
            meses = [
                (1, 'Enero'), (2, 'Febrero'), (3, 'Marzo'), (4, 'Abril'),
                (5, 'Mayo'), (6, 'Junio'), (7, 'Julio'), (8, 'Agosto'),
                (9, 'Septiembre'), (10, 'Octubre'), (11, 'Noviembre'), (12, 'Diciembre')
            ]
            cursor_destino.executemany("""
                INSERT IGNORE INTO subdim_mes (ID_Mes, NombreMes)
                VALUES (%s, %s)
            """, meses)
            conn_destino.commit()
            cursor_destino.close()
            print("   ✅ subdim_mes creada")
        
        # Verificar subdim_dia
        cursor_destino = conn_destino.cursor(buffered=True)
        cursor_destino.execute("SELECT COUNT(*) FROM subdim_dia")
        dias_count = cursor_destino.fetchone()[0]
        cursor_destino.close()
        
        if dias_count == 0:
            print("   📅 Creando subdim_dia...")
            cursor_destino = conn_destino.cursor(buffered=True)
            dias = [
                (1, 'Lunes'), (2, 'Martes'), (3, 'Miércoles'), (4, 'Jueves'),
                (5, 'Viernes'), (6, 'Sábado'), (7, 'Domingo')
            ]
            cursor_destino.executemany("""
                INSERT IGNORE INTO subdim_dia (ID_Dia, NombreDia)
                VALUES (%s, %s)
            """, dias)
            conn_destino.commit()
            cursor_destino.close()
            print("   ✅ subdim_dia creada")
        
        # PASO 4: Procesar fechas para inserción
        print("\n⏳ Procesando fechas...")
        
        datos_insercion = []
        
        for fecha in fechas:
            try:
                # Convertir a datetime si es necesario
                if isinstance(fecha, str):
                    fecha_dt = datetime.strptime(fecha, '%Y-%m-%d')
                else:
                    fecha_dt = fecha
                
                # Calcular datos de la fecha
                año = fecha_dt.year
                mes = fecha_dt.month
                dia_semana = fecha_dt.weekday() + 1  # 1=Lunes, 7=Domingo
                
                # Crear ID único para la fecha (YYYYMMDD)
                id_tiempo = int(fecha_dt.strftime('%Y%m%d'))
                
                datos_insercion.append((
                    id_tiempo,
                    fecha_dt.strftime('%Y-%m-%d'),
                    dia_semana,  # ID_Dia (1=Lunes, 7=Domingo)
                    mes,         # ID_Mes
                    año          # ID_Anio
                ))
                
            except Exception as e:
                print(f"   ❌ Error procesando fecha {fecha}: {e}")
                continue
        
        # PASO 5: Inserción por lotes
        if datos_insercion:
            print(f"\n💾 Insertando {len(datos_insercion)} registros en dim_tiempo...")
            
            cursor_destino = conn_destino.cursor(buffered=True)
            
            query_insert = """
                INSERT IGNORE INTO dim_tiempo 
                (ID_Tiempo, Fecha, ID_Dia, ID_Mes, ID_Anio)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            # Insertar en lotes de 50 registros
            batch_size = 50
            total_insertados = 0
            
            for i in range(0, len(datos_insercion), batch_size):
                batch = datos_insercion[i:i + batch_size]
                cursor_destino.executemany(query_insert, batch)
                conn_destino.commit()
                total_insertados += len(batch)
                print(f"   ✅ Lote {i//batch_size + 1} insertado ({len(batch)} registros)")
            
            cursor_destino.close()
            print(f"   📊 Total insertados: {total_insertados}")
        
        # PASO 6: Verificación final
        cursor_destino = conn_destino.cursor(buffered=True)
        cursor_destino.execute("SELECT COUNT(*) FROM dim_tiempo")
        count_final = cursor_destino.fetchone()[0]
        cursor_destino.close()
        
        print(f"\n📊 Registros finales en dim_tiempo: {count_final}")
        
        if count_final > 0:
            # Mostrar algunos ejemplos con JOIN a las subdimensiones
            cursor_destino = conn_destino.cursor(buffered=True)
            cursor_destino.execute("""
                SELECT dt.ID_Tiempo, dt.Fecha, sd.NombreDia, sm.NombreMes, sa.NumeroAnio 
                FROM dim_tiempo dt
                LEFT JOIN subdim_dia sd ON dt.ID_Dia = sd.ID_Dia
                LEFT JOIN subdim_mes sm ON dt.ID_Mes = sm.ID_Mes
                LEFT JOIN subdim_anio sa ON dt.ID_Anio = sa.ID_Anio
                ORDER BY dt.Fecha 
                LIMIT 5
            """)
            ejemplos = cursor_destino.fetchall()
            cursor_destino.close()
            
            print("\n📋 Ejemplos de registros insertados:")
            for ejemplo in ejemplos:
                print(f"   ID: {ejemplo[0]}, Fecha: {ejemplo[1]}, {ejemplo[2]}, {ejemplo[3]} {ejemplo[4]}")
        
        print("\n✅ ¡Dimensión tiempo corregida exitosamente!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error general: {e}")
        return False
    
    finally:
        # Cerrar conexiones
        if conn_origen:
            conn_origen.close()
        if conn_destino:
            conn_destino.close()
        print("✅ Conexiones cerradas")

if __name__ == "__main__":
    exito = fix_dim_tiempo()
    if exito:
        print("\n🎉 ¡Corrección completada!")
    else:
        print("\n❌ Error al corregir dimensión tiempo")