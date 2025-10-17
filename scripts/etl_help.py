#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Help - Sistema de ayuda para el ETL
"""

def print_help():
    """Imprime la ayuda del sistema ETL"""
    help_text = """
🚀 SISTEMA ETL INTELIGENTE - AYUDA
====================================

📝 DESCRIPCIÓN:
   Sistema ETL inteligente que valida automáticamente el estado del data warehouse
   y ejecuta solo las operaciones necesarias para completar el proceso.

🔧 USO:
   python scripts/etl_main.py [opciones]

⚙️  OPCIONES:
   (sin opciones)    Modo inteligente - analiza el estado y ejecuta solo lo necesario
   --rebuild         Modo reconstrucción - limpia y reprocesa todo el data warehouse
   --force           Alias para --rebuild
   --help            Muestra esta ayuda

📊 ESTADOS POSIBLES DEL DATA WAREHOUSE:
   ✅ COMPLETO       - Todo está listo, no se requiere procesamiento
   🔶 NECESITA_HECHOS - Solo falta la tabla de hechos
   🔷 NECESITA_DIMS   - Faltan dimensiones y tabla de hechos
   🔴 NECESITA_TODO   - Requiere proceso completo desde cero

🎯 FUNCIONALIDADES:
   ✅ Validación automática de catálogos y dimensiones
   ✅ Carga inteligente solo de lo que falta
   ✅ Logging detallado de cada paso
   ✅ Resumen final con estadísticas
   ✅ Modo de reconstrucción forzada
   ✅ Manejo de errores robusto

📋 EJEMPLOS DE USO:

   # Ejecución normal (recomendado)
   python scripts/etl_main.py

   # Reconstruir todo desde cero
   python scripts/etl_main.py --rebuild

   # Ver esta ayuda
   python scripts/etl_main.py --help

📁 ESTRUCTURA DEL PROYECTO:
   config/.env              - Configuración de base de datos
   scripts/etl_main.py      - Orquestador principal
   scripts/src/             - Módulos ETL
   ├── etl_validator.py     - Validación de estado
   ├── etl_catalog_loader.py - Carga de catálogos
   └── etl_complete_processor.py - Procesamiento completo

🔗 DEPENDENCIAS:
   - mysql-connector-python
   - python-dotenv
   - pathlib (incluido en Python 3.4+)

⚠️  REQUISITOS:
   - Archivo config/.env configurado correctamente
   - Conexión a base de datos origen (OLTP)
   - Conexión a base de datos destino (OLAP)
   - Esquema del data warehouse creado

📈 MÉTRICAS TÍPICAS:
   - Clientes: ~63 registros
   - Empleados: ~400 registros  
   - Proyectos: ~141 registros
   - Tareas: ~4,970 registros
   - Fechas: ~429 registros únicos
   - Finanzas: ~3,956 registros
   - Hechos: ~141 registros principales

🎉 ¡LISTO PARA ANÁLISIS DE INTELIGENCIA DE NEGOCIOS!
====================================
"""
    print(help_text)

if __name__ == "__main__":
    print_help()