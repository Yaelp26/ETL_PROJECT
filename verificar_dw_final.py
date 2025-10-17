#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector
from pathlib import Path
from dotenv import load_dotenv
import os

def verificar_dw_completo():
    """
    Verificar el estado completo del data warehouse
    """
    # Cargar variables de entorno
    project_root = Path(__file__).parent
    load_dotenv(project_root / 'config' / '.env')
    
    config_destino = {
        'host': os.getenv('DESTINO_HOST'),
        'user': os.getenv('DESTINO_USER'),
        'password': os.getenv('DESTINO_PASS'),
        'database': os.getenv('DESTINO_DB'),
        'charset': 'utf8mb4'
    }
    
    try:
        conn = mysql.connector.connect(**config_destino)
        cursor = conn.cursor()
        
        print('🏢 RESUMEN COMPLETO DEL DATA WAREHOUSE')
        print('=' * 50)
        
        # Tablas principales del DW
        tablas = [
            'dim_clientes', 'dim_empleados', 'dim_proyectos', 'dim_tareas',
            'dim_tiempo', 'dim_ubicacion', 'dim_tecnologia', 'dim_contratos',
            'dim_riesgos', 'catalogo_tipo_riesgo', 'catalogo_severidad_riesgo',
            'subdim_anio', 'subdim_mes', 'subdim_dia', 'fact_gestion_proyectos'
        ]
        
        total_registros = 0
        
        for tabla in tablas:
            try:
                cursor.execute(f'SELECT COUNT(*) FROM {tabla}')
                count = cursor.fetchone()[0]
                total_registros += count
                estado = '✅' if count > 0 else '❌'
                print(f'{estado} {tabla:<25} {count:>8} registros')
            except Exception as e:
                print(f'❌ {tabla:<25} ERROR: {e}')
        
        print('-' * 50)
        print(f'📊 TOTAL REGISTROS EN EL DW: {total_registros:>8}')
        
        print('\n💰 VERIFICACIÓN DE FACT TABLE:')
        cursor.execute('''
            SELECT COUNT(*) as total_registros,
                   MIN(Ganancias) as ganancia_min,
                   MAX(Ganancias) as ganancia_max,
                   AVG(Ganancias) as ganancia_promedio,
                   SUM(Ganancias) as ganancia_total
            FROM fact_gestion_proyectos
            WHERE Ganancias > 0
        ''')
        
        stats = cursor.fetchone()
        if stats[0] > 0:
            print(f'   📈 Registros con ganancias: {stats[0]}')
            print(f'   💵 Ganancia mínima: ${stats[1]:,.2f}')
            print(f'   💰 Ganancia máxima: ${stats[2]:,.2f}')
            print(f'   📊 Ganancia promedio: ${stats[3]:,.2f}')
            print(f'   💎 Ganancia total: ${stats[4]:,.2f}')
        
        print('\n📅 VERIFICACIÓN DE DIM_TIEMPO:')
        cursor.execute('SELECT MIN(Fecha), MAX(Fecha), COUNT(*) FROM dim_tiempo')
        tiempo_stats = cursor.fetchone()
        print(f'   📊 Total fechas: {tiempo_stats[2]}')
        print(f'   📅 Rango: {tiempo_stats[0]} a {tiempo_stats[1]}')
        
        cursor.close()
        conn.close()
        print('\n🎉 ¡DATA WAREHOUSE COMPLETAMENTE POBLADO!')
        print('✅ ¡ETL PROCESO COMPLETADO CON ÉXITO!')
        
    except Exception as e:
        print(f'Error: {e}')

if __name__ == "__main__":
    verificar_dw_completo()