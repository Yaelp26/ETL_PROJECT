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
    Corrige la dimensión tiempo resolviendo el problema de cursor
    """
    print("📅 CORRIGIENDO DIMENSIÓN TIEMPO V2")
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
        'buffered': True  # Importante para evitar "Unread result found"
    }
    
    config_destino = {
        'host': os.getenv('DESTINO_HOST'),
        'user': os.getenv('DESTINO_USER'),
        'password': os.getenv('DESTINO_PASS'),
        'database': os.getenv('DESTINO_DB'),
        'charset': 'utf8mb4',
        'buffered': True  # Importante para evitar "Unread result found"
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
        
        # PASO 1: Asegurar que subdim_anio tenga los años
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
        
        # Insertar años en subdim_anio
        cursor_destino = conn_destino.cursor(buffered=True)
        for año in años:
            cursor_destino.execute("""
                INSERT IGNORE INTO subdim_anio (IdAnio, Anio)
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
        
        # PASO 3: Procesar fechas en lotes para evitar problemas de cursor
        print("\n⏳ Procesando fechas en lotes...")
        
        # Preparar los datos para inserción por lotes
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
                dia = fecha_dt.day
                nombre_mes = calendar.month_name[mes]
                nombre_dia = calendar.day_name[fecha_dt.weekday()]
                
                # Obtener IDs de las subdimensiones
                id_año = año  # Asumiendo que IdAnio = Anio
                id_mes = mes   # Asumiendo que IdMes = numero del mes
                id_dia = fecha_dt.weekday() + 1  # 1=Lunes, 7=Domingo
                
                # Crear ID único para la fecha (YYYYMMDD)
                id_tiempo = int(fecha_dt.strftime('%Y%m%d'))
                
                datos_insercion.append((
                    id_tiempo,
                    fecha_dt.strftime('%Y-%m-%d'),
                    dia,
                    mes,
                    año,
                    nombre_dia,
                    nombre_mes,
                    id_año,
                    id_mes,
                    id_dia
                ))
                
            except Exception as e:
                print(f"   ❌ Error procesando fecha {fecha}: {e}")
                continue
        
        # PASO 4: Inserción por lotes
        if datos_insercion:
            print(f"\n💾 Insertando {len(datos_insercion)} registros en dim_tiempo...")
            
            cursor_destino = conn_destino.cursor(buffered=True)
            
            query_insert = """
                INSERT IGNORE INTO dim_tiempo 
                (IdTiempo, Fecha, Dia, Mes, Anio, NombreDia, NombreMes, IdAnio, IdMes, IdDia)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Insertar en lotes de 50 registros
            batch_size = 50
            for i in range(0, len(datos_insercion), batch_size):
                batch = datos_insercion[i:i + batch_size]
                cursor_destino.executemany(query_insert, batch)
                conn_destino.commit()
                print(f"   ✅ Lote {i//batch_size + 1} insertado ({len(batch)} registros)")
            
            cursor_destino.close()
        
        # PASO 5: Verificación final
        cursor_destino = conn_destino.cursor(buffered=True)
        cursor_destino.execute("SELECT COUNT(*) FROM dim_tiempo")
        count_final = cursor_destino.fetchone()[0]
        cursor_destino.close()
        
        print(f"\n📊 Registros finales en dim_tiempo: {count_final}")
        
        if count_final > 0:
            # Mostrar algunos ejemplos
            cursor_destino = conn_destino.cursor(buffered=True)
            cursor_destino.execute("""
                SELECT IdTiempo, Fecha, NombreDia, NombreMes, Anio 
                FROM dim_tiempo 
                ORDER BY Fecha 
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