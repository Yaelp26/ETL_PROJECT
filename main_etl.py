"""
Orquesta el proceso completo de Extract, Transform, Load
"""
import logging
from typing import Dict
import pandas as pd

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Imports de módulos ETL
from extract.extract_gestion import extract_all, reset_incremental_control, get_last_extraction_info

# Imports de transformaciones individuales - Dimensiones
from transform.transform_dim.dim_clientes import transform as transform_dim_clientes
from transform.transform_dim.dim_empleados import transform as transform_dim_empleados
from transform.transform_dim.dim_proyectos import transform as transform_dim_proyectos
from transform.transform_dim.dim_tiempo import transform as transform_dim_tiempo
from transform.transform_dim.dim_hitos import transform as transform_dim_hitos
from transform.transform_dim.dim_tareas import transform as transform_dim_tareas
from transform.transform_dim.dim_pruebas import transform as transform_dim_pruebas
from transform.transform_dim.dim_finanzas import transform as transform_dim_finanzas
from transform.transform_dim.dim_tipo_riesgo import transform as transform_dim_tipo_riesgo
from transform.transform_dim.dim_severidad import transform as transform_dim_severidad
from transform.transform_dim.dim_riesgos import transform as transform_dim_riesgos

# Imports de transformaciones - Hechos
from transform.transform_fact.hechos_asignaciones import transform as transform_hechos_asignaciones
from transform.transform_fact.hechos_proyectos import transform as transform_hechos_proyectos

# from load.load_to_dw import load_all  # Comentado hasta implementar

def run_transformations(raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    transformed_data = {}
    
    logger.info("=== INICIANDO TRANSFORMACIONES ===")
    
    # Transformar dimensiones
    logger.info("--- Transformando Dimensiones ---")
    
    try:
        # Dimensiones
        transformed_data['dim_clientes'] = transform_dim_clientes(raw_data)
        transformed_data['dim_empleados'] = transform_dim_empleados(raw_data)
        transformed_data['dim_proyectos'] = transform_dim_proyectos(raw_data)
        transformed_data['dim_tiempo'] = transform_dim_tiempo(raw_data)
        transformed_data['dim_hitos'] = transform_dim_hitos(raw_data)
        transformed_data['dim_tareas'] = transform_dim_tareas(raw_data)
        transformed_data['dim_pruebas'] = transform_dim_pruebas(raw_data)
        transformed_data['dim_finanzas'] = transform_dim_finanzas(raw_data)
        transformed_data['dim_tipo_riesgo'] = transform_dim_tipo_riesgo(raw_data)
        transformed_data['dim_severidad'] = transform_dim_severidad(raw_data)
        transformed_data['dim_riesgos'] = transform_dim_riesgos(raw_data)
        
        # Transformar hechos (necesitan tanto datos crudos como dimensiones)
        logger.info("--- Transformando Hechos ---")
        # Combinar datos crudos con dimensiones transformadas para los hechos
        combined_data = {**raw_data, **transformed_data}
        transformed_data['hechos_asignaciones'] = transform_hechos_asignaciones(combined_data)
        transformed_data['hechos_proyectos'] = transform_hechos_proyectos(combined_data)
        
        # Resumen de transformación
        logger.info("--- Resumen de Transformaciones ---")
        for table_name, df in transformed_data.items():
            logger.info(f"{table_name}: {len(df)} registros transformados")
            
    except Exception as e:
        logger.error(f"Error en transformaciones: {str(e)}")
        raise
    
    logger.info("=== TRANSFORMACIONES COMPLETADAS ===")
    return transformed_data

def run_extract_transform(incremental: bool = True):
    mode_msg = "INCREMENTAL" if incremental else "COMPLETA"
    logger.info(f" INICIANDO ETL {mode_msg} - EXTRACCIÓN Y TRANSFORMACIÓN")
    
    try:
        # Mostrar info de última extracción
        if incremental:
            last_date = get_last_extraction_info()
            logger.info(f" Última extracción: {last_date}")
        
        # 1. Extracción
        logger.info(f" FASE 1: EXTRACCIÓN {mode_msg}")
        raw_data = extract_all(incremental=incremental)
        
        if not raw_data:
            logger.warning("No se extrajeron datos. Finalizando proceso.")
            return None
        
        # 2. Transformación
        logger.info(" FASE 2: TRANSFORMACIÓN")
        transformed_data = run_transformations(raw_data)
        
        logger.info(f" ETL {mode_msg} (Extract + Transform) completado exitosamente")
        return transformed_data
        
    except Exception as e:
        logger.error(f" Error en ETL {mode_msg}: {str(e)}")
        raise

def run_etl():
    """
    Ejecuta el proceso ETL completo
    """
    logger.info(" INICIANDO ETL COMPLETO")
    
    try:
        # Extraer y transformar
        transformed_data = run_extract_transform()
        
        if transformed_data is None:
            return
        
        # 3. Carga (por implementar)
        logger.info(" FASE 3: CARGA")
        logger.warning("Módulo de carga aún no implementado")
        # load_all(transformed_data)
        
        logger.info(" ETL completo finalizado")
        
    except Exception as e:
        logger.error(f" Error en ETL completo: {str(e)}")
        raise

def test_etl():
    """
    Función de prueba del ETL
    """
    print(" EJECUTANDO PRUEBA DE ETL")
    try:
        result = run_extract_transform()
        
        if result:
            print("\n📊 RESULTADOS DE LA PRUEBA:")
            for table_name, df in result.items():
                print(f"\n--- {table_name.upper()} ---")
                print(f"Registros: {len(df)}")
                if len(df) > 0:
                    print("Primeras 3 filas:")
                    print(df.head(3))
                else:
                    print("No hay datos")
        
        print("\n Prueba completada exitosamente")
        return result
        
    except Exception as e:
        print(f"\n Error en prueba: {str(e)}")
        return None

def run_full_load():
    """
    Ejecutar carga completa (no incremental)
    """
    logger.info("🔄 FORZANDO CARGA COMPLETA")
    return run_extract_transform(incremental=False)

def reset_and_run():
    """
    Resetear control incremental y ejecutar carga completa
    """
    logger.info(" RESETEANDO CONTROL INCREMENTAL")
    reset_incremental_control()
    return run_full_load()

def show_incremental_status():
    """
    Mostrar estado del control incremental
    """
    last_date = get_last_extraction_info()
    print(f" Última extracción: {last_date}")
    print(f" Próxima extracción será: INCREMENTAL (solo cambios desde {last_date})")
    print(" Para carga completa usar: reset_and_run() o run_full_load()")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--full":
            print("Ejecutando carga completa...")
            run_full_load()
        elif sys.argv[1] == "--reset":
            print("Reseteando control y ejecutando carga completa...")
            reset_and_run()
        elif sys.argv[1] == "--status":
            show_incremental_status()
        else:
            print("Opciones: --full, --reset, --status")
    else:
        # Ejecución normal (incremental)
        test_etl()
