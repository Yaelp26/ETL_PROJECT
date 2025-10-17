#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Main - Proceso principal reproducible de ETL para Data Warehouse
Autor: Sistema ETL Inteligente
Descripción: Orquesta todo el proceso ETL validando el estado actual y ejecutando solo lo necesario
"""

import sys
import os
from datetime import datetime
from pathlib import Path

# Agregar el directorio src al path
sys.path.append(str(Path(__file__).parent / 'src'))

from etl_validator import ETLValidator
from etl_catalog_loader import ETLCatalogLoader
from etl_complete_processor import ETLCompleteProcessor

class ETLOrchestrator:
    """Orquestador principal del proceso ETL"""
    
    def __init__(self):
        self.validator = ETLValidator()
        self.catalog_loader = ETLCatalogLoader()
        self.processor = ETLCompleteProcessor()
        self.start_time = None
        self.execution_log = []
    
    def log_step(self, step, status, message=""):
        """Registra un paso del proceso"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = {
            'timestamp': timestamp,
            'step': step,
            'status': status,
            'message': message
        }
        self.execution_log.append(log_entry)
        
        status_icon = "✅" if status == "SUCCESS" else "❌" if status == "ERROR" else "⏳"
        print(f"{status_icon} [{timestamp}] {step}: {message}")
    
    def print_banner(self):
        """Imprime el banner inicial"""
        print("=" * 60)
        print("🚀 SISTEMA ETL INTELIGENTE - DATA WAREHOUSE")
        print("=" * 60)
        print("📊 Proyecto: Gestión de Proyectos OLTP → OLAP")
        print("🔄 Modo: Reproducible con validación automática")
        print("📅 Configuración: Variables de entorno (.env)")
        print("=" * 60)
    
    def validate_environment(self):
        """Valida que el entorno esté configurado correctamente"""
        self.log_step("VALIDACIÓN ENTORNO", "RUNNING", "Verificando configuración...")
        
        # Verificar archivo .env
        env_path = Path(__file__).parent.parent / 'config' / '.env'
        if not env_path.exists():
            self.log_step("VALIDACIÓN ENTORNO", "ERROR", "Archivo .env no encontrado")
            return False
        
        # Verificar variables requeridas
        from dotenv import load_dotenv
        load_dotenv(env_path)
        
        required_vars = [
            'ORIGEN_HOST', 'ORIGEN_USER', 'ORIGEN_PASS', 'ORIGEN_DB',
            'DESTINO_HOST', 'DESTINO_USER', 'DESTINO_PASS', 'DESTINO_DB'
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            self.log_step("VALIDACIÓN ENTORNO", "ERROR", f"Variables faltantes: {missing_vars}")
            return False
        
        self.log_step("VALIDACIÓN ENTORNO", "SUCCESS", "Configuración válida")
        return True
    
    def analyze_current_state(self):
        """Analiza el estado actual del data warehouse"""
        self.log_step("ANÁLISIS ESTADO", "RUNNING", "Analizando estado del DW...")
        
        status = self.validator.get_full_status()
        
        if status['etl_ready']:
            self.log_step("ANÁLISIS ESTADO", "SUCCESS", "Data Warehouse ya está completo")
            return 'COMPLETE'
        elif status['catalogs_ready'] and status['dimensions_ready']:
            self.log_step("ANÁLISIS ESTADO", "SUCCESS", "Solo falta tabla de hechos")
            return 'NEED_FACTS'
        elif status['catalogs_ready']:
            self.log_step("ANÁLISIS ESTADO", "SUCCESS", "Solo faltan dimensiones y hechos")
            return 'NEED_DIMENSIONS'
        else:
            self.log_step("ANÁLISIS ESTADO", "SUCCESS", "Requiere proceso completo")
            return 'NEED_ALL'
    
    def execute_catalog_loading(self):
        """Ejecuta la carga de catálogos"""
        self.log_step("CARGA CATÁLOGOS", "RUNNING", "Poblando catálogos...")
        
        if self.catalog_loader.load_all_catalogs():
            self.log_step("CARGA CATÁLOGOS", "SUCCESS", "Catálogos poblados exitosamente")
            return True
        else:
            self.log_step("CARGA CATÁLOGOS", "ERROR", "Error poblando catálogos")
            return False
    
    def execute_dimension_loading(self):
        """Ejecuta la carga de dimensiones"""
        self.log_step("CARGA DIMENSIONES", "RUNNING", "Poblando dimensiones...")
        
        if self.processor.load_dimensions():
            self.log_step("CARGA DIMENSIONES", "SUCCESS", "Dimensiones pobladas exitosamente")
            return True
        else:
            self.log_step("CARGA DIMENSIONES", "ERROR", "Error poblando dimensiones")
            return False
    
    def execute_fact_loading(self):
        """Ejecuta la carga de la tabla de hechos"""
        self.log_step("CARGA HECHOS", "RUNNING", "Poblando tabla de hechos...")
        
        if self.processor.load_fact_table():
            self.log_step("CARGA HECHOS", "SUCCESS", "Tabla de hechos poblada exitosamente")
            return True
        else:
            self.log_step("CARGA HECHOS", "ERROR", "Error poblando tabla de hechos")
            return False
    
    def show_final_summary(self):
        """Muestra el resumen final del proceso"""
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        print("\n" + "=" * 60)
        print("📊 RESUMEN FINAL DEL PROCESO ETL")
        print("=" * 60)
        
        # Mostrar estadísticas del proceso
        success_steps = len([log for log in self.execution_log if log['status'] == 'SUCCESS'])
        error_steps = len([log for log in self.execution_log if log['status'] == 'ERROR'])
        
        print(f"⏱️  Duración total: {duration}")
        print(f"✅ Pasos exitosos: {success_steps}")
        print(f"❌ Pasos con error: {error_steps}")
        
        # Validación final
        print("\n🔍 VALIDACIÓN FINAL:")
        final_status = self.validator.get_full_status()
        
        if final_status['etl_ready']:
            print("🎉 ¡DATA WAREHOUSE COMPLETAMENTE FUNCIONAL!")
            print(f"📈 El ETL está listo para análisis de inteligencia de negocios")
        else:
            print("⚠️  El proceso no se completó exitosamente")
            print("🔧 Revise los errores reportados arriba")
        
        print("=" * 60)
    
    def run(self, force_rebuild=False):
        """Ejecuta el proceso ETL completo de manera inteligente"""
        self.start_time = datetime.now()
        self.print_banner()
        
        try:
            # 1. Validar entorno
            if not self.validate_environment():
                return False
            
            # 2. Analizar estado actual (solo si no es rebuild forzado)
            if not force_rebuild:
                current_state = self.analyze_current_state()
            else:
                current_state = 'NEED_ALL'
                self.log_step("MODO FORZADO", "RUNNING", "Reconstrucción completa solicitada")
            
            # 3. Ejecutar según el estado
            if current_state == 'COMPLETE':
                self.log_step("PROCESO ETL", "SUCCESS", "Data Warehouse ya está completo - No se requiere procesamiento")
                
            elif current_state == 'NEED_FACTS':
                self.log_step("PROCESO ETL", "RUNNING", "Ejecutando carga de tabla de hechos únicamente")
                success = self.execute_fact_loading()
                
            elif current_state == 'NEED_DIMENSIONS':
                self.log_step("PROCESO ETL", "RUNNING", "Ejecutando carga de dimensiones y hechos")
                success = (self.execute_dimension_loading() and 
                          self.execute_fact_loading())
                
            elif current_state == 'NEED_ALL':
                self.log_step("PROCESO ETL", "RUNNING", "Ejecutando proceso ETL completo")
                success = (self.execute_catalog_loading() and 
                          self.execute_dimension_loading() and 
                          self.execute_fact_loading())
            
            # 4. Mostrar resumen final
            self.show_final_summary()
            
            return current_state == 'COMPLETE' or success
            
        except Exception as e:
            self.log_step("PROCESO ETL", "ERROR", f"Error inesperado: {e}")
            return False

def main():
    """Función principal"""
    # Verificar ayuda
    if '--help' in sys.argv or '-h' in sys.argv:
        from etl_help import print_help
        print_help()
        sys.exit(0)
    
    # Verificar argumentos de línea de comandos
    force_rebuild = '--rebuild' in sys.argv or '--force' in sys.argv
    
    if force_rebuild:
        print("🔄 Modo de reconstrucción forzada activado")
    
    # Crear y ejecutar el orquestador
    orchestrator = ETLOrchestrator()
    success = orchestrator.run(force_rebuild=force_rebuild)
    
    # Código de salida
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()
