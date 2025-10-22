"""
Módulo de Carga (Load) - ETL Proyecto Escolar
Carga datos transformados al Data Warehouse (MySQL)
"""
import pandas as pd
import mysql.connector
from mysql.connector import Error as MySQLError
import logging
from typing import Dict
import sys
import os

# Agregar path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.db_config import DB_DW

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class DWLoader:
    def __init__(self):
        self.connection = None
        self.cursor = None
    
    def connect(self):
        try:
            self.connection = mysql.connector.connect(**DB_DW)
            self.cursor = self.connection.cursor()
            logger.info("Conexión establecida con DW exitosamente")
            return True
        except Exception as e:
            logger.error(f"Error conectando al DW: {str(e)}")
            return False
    
    def disconnect(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.info("Conexión cerrada exitosamente")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {str(e)}")
    
    def create_table_if_not_exists(self, table_name: str, schema: str):
        try:
            create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
            self.cursor.execute(create_query)
            self.connection.commit()
            logger.debug(f"Tabla {table_name} verificada/creada")
            return True
        except mysql.connector.Error as e:
            if e.errno == 1142:  # CREATE command denied
                logger.warning(f"Sin permisos CREATE para {table_name}. Asumiendo que la tabla existe.")
                return False
            else:
                logger.error(f"Error creando tabla {table_name}: {str(e)}")
                raise
        except Exception as e:
            logger.error(f"Error creando tabla {table_name}: {str(e)}")
            raise
    
    def truncate_table(self, table_name: str):
        try:
            # Deshabilitar verificaciones FK temporalmente
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            self.cursor.execute(f"TRUNCATE TABLE {table_name}")
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            self.connection.commit()
            logger.debug(f"Tabla {table_name} vaciada")
            return True
        except mysql.connector.Error as e:
            if e.errno == 1142:  # Permission denied
                logger.warning(f"Sin permisos TRUNCATE para {table_name}. Usando DELETE.")
                try:
                    self.cursor.execute(f"DELETE FROM {table_name}")
                    self.connection.commit()
                    logger.debug(f"Tabla {table_name} vaciada con DELETE")
                    return True
                except Exception as delete_error:
                    logger.warning(f"Error vaciando tabla {table_name}: {str(delete_error)}")
                    return False
            else:
                logger.warning(f"Error vaciando tabla {table_name}: {str(e)}")
                return False
        except Exception as e:
            logger.warning(f"Error vaciando tabla {table_name}: {str(e)}")
            return False
    
    def convert_pandas_types(self, df: pd.DataFrame) -> pd.DataFrame:
        df_converted = df.copy()
        
        # Convertir todos los valores a tipos Python nativos
        for col in df_converted.columns:
            
            # Manejar fechas especialmente
            if df_converted[col].dtype.name == 'object':
                # Verificar si es una columna de fechas
                first_non_null = df_converted[col].dropna().iloc[0] if not df_converted[col].dropna().empty else None
                if first_non_null and hasattr(first_non_null, 'date'):
                    # Es una fecha, convertir a string en formato MySQL
                    df_converted[col] = df_converted[col].apply(lambda x: x.strftime('%Y-%m-%d') if x is not None else None)
                else:
                    # Es string u object regular
                    df_converted[col] = df_converted[col].where(pd.notnull(df_converted[col]), None)
            
            # Luego convertir tipos numéricos
            elif df_converted[col].dtype.name.startswith('int'):
                # Convertir cualquier tipo de entero a int Python
                df_converted[col] = df_converted[col].apply(lambda x: int(x) if pd.notnull(x) else None)
            elif df_converted[col].dtype.name.startswith('float'):
                # Convertir cualquier tipo de float a float Python
                df_converted[col] = df_converted[col].apply(lambda x: float(x) if pd.notnull(x) else None)
            elif df_converted[col].dtype.name == 'bool':
                # Convertir bool a bool Python
                df_converted[col] = df_converted[col].apply(lambda x: bool(x) if pd.notnull(x) else None)
            else:
                # Para otros tipos, simplemente reemplazar NaN con None
                df_converted[col] = df_converted[col].where(pd.notnull(df_converted[col]), None)
        
        return df_converted
    
    def load_dataframe_to_table(self, df: pd.DataFrame, table_name: str, mode: str = 'replace'):
        if df.empty:
            logger.warning(f"DataFrame vacío para tabla {table_name}")
            return 0
        
        try:
            # Debug: mostrar datos antes de conversión
            logger.debug(f"ANTES conversión - Tabla: {table_name}")
            logger.debug(f"  Columnas: {list(df.columns)}")
            logger.debug(f"  Tipos: {df.dtypes.to_dict()}")
            
            # Convertir tipos de pandas a tipos compatibles con MySQL
            df_converted = self.convert_pandas_types(df)
            
            # Limpiar columnas no deseadas que pueden crearse por pandas
            # Eliminar cualquier columna con sufijo '_date' o similar que no esté en el original
            original_columns = set(df.columns)
            current_columns = set(df_converted.columns)
            extra_columns = current_columns - original_columns
            
            # También buscar columnas con patrones problemáticos
            suspicious_columns = [col for col in df_converted.columns if '_date' in col.lower()]
            
            if extra_columns:
                logger.warning(f"Eliminando columnas extra generadas: {extra_columns}")
                df_converted = df_converted.drop(columns=list(extra_columns))
            
            if suspicious_columns:
                logger.warning(f"Eliminando columnas sospechosas: {suspicious_columns}")
                df_converted = df_converted.drop(columns=suspicious_columns, errors='ignore')
            
            # Debug: mostrar datos después de conversión
            logger.debug(f"DESPUÉS conversión - Tabla: {table_name}")
            logger.debug(f"  Columnas: {list(df_converted.columns)}")
            logger.debug(f"  Tipos: {df_converted.dtypes.to_dict()}")
            
            # Preparar datos
            records_to_insert = len(df_converted)
            
            # Convertir DataFrame a lista de tuplas (aplicando conversión de tipos)
            converted_rows = []
            for _, row in df_converted.iterrows():
                converted_row = []
                for value in row:
                    if pd.isna(value):
                        converted_row.append(None)
                    elif hasattr(value, 'item'):  # Para tipos numpy
                        converted_row.append(value.item())  # Convierte numpy.int64 a int Python
                    else:
                        converted_row.append(value)
                converted_rows.append(tuple(converted_row))
            
            data_tuples = converted_rows
            
            # Crear placeholders para INSERT
            placeholders = ', '.join(['%s'] * len(df_converted.columns))
            columns = ', '.join(df_converted.columns)
            
            # Debug: mostrar información sobre la tabla
            logger.debug(f"Tabla: {table_name}")
            logger.debug(f"Columnas DataFrame: {list(df_converted.columns)}")
            logger.debug(f"Número de columnas: {len(df_converted.columns)}")
            logger.debug(f"Número de placeholders: {placeholders.count('%s')}")
            
            if mode == 'replace':
                # Vaciar tabla antes de insertar
                logger.debug(f"Vaciando tabla {table_name}...")
                self.truncate_table(table_name)
            
            # INSERT datos
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # Insertar en lotes para mejor rendimiento
            batch_size = 1000
            total_inserted = 0
            
            for i in range(0, len(data_tuples), batch_size):
                batch = data_tuples[i:i + batch_size]
                self.cursor.executemany(insert_query, batch)
                total_inserted += len(batch)
                
                if total_inserted % 5000 == 0:
                    logger.debug(f"{table_name}: {total_inserted}/{records_to_insert} registros insertados")
            
            self.connection.commit()
            logger.info(f" {table_name}: {total_inserted} registros cargados exitosamente")
            return total_inserted
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f" Error cargando {table_name}: {str(e)}")
            raise

def get_table_schemas():
    """Definir esquemas de todas las tablas del DW"""
    return {
        # DIMENSIONES
        'dim_clientes': """
            ID_Cliente INT PRIMARY KEY,
            CodigoClienteReal INT NOT NULL
        """,
        
        'dim_empleados': """
            ID_Empleado INT PRIMARY KEY,
            CodigoEmpleado INT NOT NULL,
            Rol VARCHAR(50),
            Seniority VARCHAR(50)
        """,
        
        'dim_proyectos': """
            ID_Proyecto INT PRIMARY KEY,
            CodigoProyecto INT NOT NULL,
            Version VARCHAR(50),
            Cancelado TINYINT,
            ID_Cliente INT,
            INDEX idx_cliente (ID_Cliente)
        """,
        
        'dim_tiempo': """
            ID_Tiempo INT PRIMARY KEY,
            Fecha DATE NOT NULL,
            ID_DiaSemana INT,
            ID_Mes INT,
            ID_Anio INT,
            INDEX idx_fecha (Fecha)
        """,
        
        'dim_hitos': """
            ID_Hito INT PRIMARY KEY,
            CodigoHito INT NOT NULL,
            ID_Proyecto INT,
            ID_FechaInicio INT,
            ID_FechaFinalizacion INT,
            Retraso_days INT,
            INDEX idx_proyecto (ID_Proyecto)
        """,
        
        'dim_tareas': """
            ID_Tarea INT PRIMARY KEY,
            CodigoTarea INT NOT NULL,
            ID_Hito INT,
            DuracionDias INT,
            RetrasoDias INT,
            INDEX idx_hito (ID_Hito)
        """,
        
        'dim_pruebas': """
            ID_Prueba INT PRIMARY KEY,
            CodigoPrueba INT NOT NULL,
            ID_Hito INT,
            TipoPrueba VARCHAR(50),
            PruebaExitosa TINYINT,
            INDEX idx_hito (ID_Hito)
        """,
        
        'dim_finanzas': """
            ID_Finanza INT PRIMARY KEY,
            TipoGasto VARCHAR(100),
            Categoria VARCHAR(50),
            Monto DECIMAL(15,2)
        """,
        
        'dim_tipo_riesgo': """
            ID_TipoRiesgo INT PRIMARY KEY,
            NombreTipo VARCHAR(100) NOT NULL
        """,
        
        'dim_severidad': """
            ID_Severidad INT PRIMARY KEY,
            Nivel VARCHAR(50) NOT NULL
        """,
        
        'dim_riesgos': """
            ID_Riesgo INT PRIMARY KEY,
            ID_TipoRiesgo INT,
            ID_Severidad INT,
            INDEX idx_tipo (ID_TipoRiesgo),
            INDEX idx_severidad (ID_Severidad)
        """,
        
        # TABLAS DE HECHOS
        'hechos_asignaciones': """
            ID_HechoAsignacion INT PRIMARY KEY,
            ID_Empleado INT,
            ID_Proyecto INT,
            ID_FechaAsignacion INT,
            HorasPlanificadas DECIMAL(10,2),
            HorasReales DECIMAL(10,2),
            ValorHoras DECIMAL(15,2),
            INDEX idx_empleado (ID_Empleado),
            INDEX idx_proyecto (ID_Proyecto),
            INDEX idx_fecha (ID_FechaAsignacion)
        """,
        
        'hechos_proyectos': """
            ID_Hecho INT PRIMARY KEY,
            ID_Proyecto INT,
            ID_TiempoInicio INT,
            ID_TiempoFinalizacion INT,
            ID_Riesgo INT,
            ID_Finanza INT,
            DuracionRealDias INT,
            RetrasoDias INT,
            PresupuestoCliente DECIMAL(15,2),
            CosteReal DECIMAL(15,2),
            DesviacionPresupuestal DECIMAL(15,2),
            PenalizacionesMonto DECIMAL(15,2),
            ProporcionCAPEX_OPEX DECIMAL(10,4),
            NumeroDefectosEncontrados INT,
            ProductividadPromedio DECIMAL(10,2),
            PorcentajeTareasRetrasadas DECIMAL(5,2),
            PorcentajeHitosRetrasados DECIMAL(5,2),
            INDEX idx_proyecto (ID_Proyecto),
            INDEX idx_tiempo_inicio (ID_TiempoInicio),
            INDEX idx_tiempo_fin (ID_TiempoFinalizacion)
        """
    }

def load_all_to_dw(transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, int]:
    """
    Cargar todos los datos transformados al Data Warehouse
    (Asume que el esquema del DW ya existe)
    
    Args:
        transformed_data: Diccionario con todas las tablas transformadas
        
    Returns:
        Dict con conteo de registros cargados por tabla
    """
    loader = DWLoader()
    load_results = {}
    
    # Orden de carga: DIMENSIONES primero, luego HECHOS
    load_order = [
        # Dimensiones independientes primero
        'dim_clientes', 'dim_empleados', 'dim_tiempo', 'dim_tipo_riesgo', 'dim_severidad',
        # Dimensiones dependientes
        'dim_proyectos', 'dim_finanzas', 'dim_riesgos', 'dim_hitos', 'dim_tareas', 'dim_pruebas',
        # Tablas de hechos al final
        'hechos_asignaciones', 'hechos_proyectos'
    ]
    
    try:
        if not loader.connect():
            raise Exception("No se pudo conectar al Data Warehouse")
        
        logger.info("🚀INICIANDO CARGA AL DATA WAREHOUSE")
        logger.info(f" Tablas a cargar: {len(load_order)}")
        logger.info("Asumiendo que el esquema del DW ya existe")
        
        total_records = 0
        
        for table_name in load_order:
            if table_name in transformed_data:
                df = transformed_data[table_name]
                
                # Cargar datos directamente (sin crear tablas)
                records_loaded = loader.load_dataframe_to_table(df, table_name, mode='replace')
                load_results[table_name] = records_loaded
                total_records += records_loaded
                
            else:
                logger.warning(f"warning Tabla {table_name} no encontrada en datos transformados")
                load_results[table_name] = 0
        
        logger.info("=" * 50)
        logger.info(" RESUMEN DE CARGA:")
        for table, count in load_results.items():
            status = "ok" if count > 0 else "warning"
            logger.info(f"{status} {table}: {count:,} registros")
        
        logger.info(f" TOTAL REGISTROS CARGADOS: {total_records:,}")
        logger.info(" CARGA AL DATA WAREHOUSE COMPLETADA EXITOSAMENTE")
        
    except Exception as e:
        logger.error(f" Error en carga al DW: {str(e)}")
        raise
    finally:
        loader.disconnect()
    
    return load_results