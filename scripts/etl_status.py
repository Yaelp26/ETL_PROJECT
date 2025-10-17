#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Status - Verificación rápida del estado del data warehouse
"""

import sys
from pathlib import Path

# Agregar el directorio src al path
sys.path.append(str(Path(__file__).parent / 'src'))

from etl_validator import ETLValidator

def quick_status():
    """Verificación rápida y concisa del estado"""
    print("🔍 ESTADO RÁPIDO DEL DATA WAREHOUSE")
    print("=" * 40)
    
    validator = ETLValidator()
    status = validator.get_full_status()
    
    if status['etl_ready']:
        print("✅ ESTADO: COMPLETO Y FUNCIONAL")
        print("🎯 Acción requerida: Ninguna")
        print("💡 El ETL está listo para análisis")
    else:
        print("❌ ESTADO: REQUIERE PROCESAMIENTO")
        
        if not status['catalogs_ready']:
            print("📚 Catálogos: ❌ Incompletos")
        if not status['dimensions_ready']:
            print("📊 Dimensiones: ❌ Incompletas")  
        if not status['fact_ready']:
            print("🎯 Hechos: ❌ Incompletos")
            
        print("💡 Ejecutar: python scripts/etl_main.py")
    
    print("=" * 40)
    return status['etl_ready']

if __name__ == "__main__":
    ready = quick_status()
    sys.exit(0 if ready else 1)