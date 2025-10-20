"""
Módulo de extracción de datos del Sistema de Gestión de Proyectos (SGP)
Extrae datos siguiendo el orden de dependencias de las tablas

REGLA DE NEGOCIO CRÍTICA:
- SOLO se extraen datos de proyectos con Estado = 'Terminado' OR 'Cancelado'
- O de contratos con Estado = 'Terminado' OR 'Cancelado'
- Esta regla se aplica en cascada a todas las tablas relacionadas
"""

import mysql.connector
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, Any
import sys
import os

# Agregar el directorio padre al path para importar módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.db_config import DB_OLTP
from utils.helpers import get_connection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SGPExtractor:
    """Clase para extraer datos del Sistema de Gestión de Proyectos"""
    
    def __init__(self):
        self.connection = None
        self.extraction_timestamp = datetime.now()
        
    def connect(self):
        """Establecer conexión con la base de datos SGP"""
        try:
            self.connection = get_connection("OLTP")
            logger.info("Conexión establecida con SGP exitosamente")
            return True
        except Exception as e:
            logger.error(f"Error conectando a SGP: {str(e)}")
            return False
    
    def disconnect(self):
        """Cerrar conexión con la base de datos"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Conexión cerrada exitosamente")
    
    def execute_query(self, query: str, table_name: str) -> pd.DataFrame:
        """Ejecutar consulta y retornar DataFrame"""
        try:
            df = pd.read_sql(query, self.connection)
            logger.info(f"Extraídos {len(df)} registros de {table_name}")
            return df
        except Exception as e:
            logger.error(f"Error extrayendo datos de {table_name}: {str(e)}")
            return pd.DataFrame()

    # ================= TABLAS MAESTRO =================
    
    def extract_clientes(self) -> pd.DataFrame:
        """Extraer datos de clientes - SOLO clientes con contratos terminados o cancelados"""
        query = """
        SELECT DISTINCT
            cl.ID_Cliente,
            cl.NombreCliente,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM clientes cl
        INNER JOIN contratos c ON cl.ID_Cliente = c.ID_Cliente
        WHERE c.Estado IN ('Terminado', 'Cancelado')
        ORDER BY cl.ID_Cliente
        """
        return self.execute_query(query, "clientes")
    
    def extract_empleados(self) -> pd.DataFrame:
        """Extraer datos de empleados - SOLO empleados asignados a proyectos terminados o cancelados"""
        query = """
        SELECT DISTINCT
            e.ID_Empleado,
            e.NombreCompleto,
            e.Rol,
            e.Seniority,
            e.CostoPorHora,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM empleados e
        INNER JOIN asignaciones a ON e.ID_Empleado = a.ID_Empleado
        INNER JOIN proyectos p ON a.ID_Proyecto = p.ID_Proyecto
        INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
        WHERE (p.Estado IN ('Terminado', 'Cancelado') 
               OR c.Estado IN ('Terminado', 'Cancelado'))
        ORDER BY e.ID_Empleado
        """
        return self.execute_query(query, "empleados")
    
    def extract_contratos(self) -> pd.DataFrame:
        """Extraer datos de contratos - SOLO contratos terminados o cancelados"""
        query = """
        SELECT 
            ID_Contrato,
            ID_Cliente,
            ValorTotalContrato,
            Estado,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM contratos
        WHERE Estado IN ('Terminado', 'Cancelado')
        ORDER BY ID_Contrato
        """
        return self.execute_query(query, "contratos")
    
    def extract_proyectos(self) -> pd.DataFrame:
        """
        Extraer datos de proyectos - REGLA DE NEGOCIO:
        - Proyecto terminado/cancelado: Se incluye
        - Contrato terminado/cancelado: Se incluyen todos sus proyectos
        """
        query = """
        SELECT 
            p.ID_Proyecto,
            p.ID_Contrato,
            p.NombreProyecto,
            p.Version,
            p.FechaInicio,
            p.FechaFin,
            p.Estado as EstadoProyecto,
            c.ID_Cliente,
            c.ValorTotalContrato,
            c.Estado as EstadoContrato,
            CASE 
                WHEN p.Estado IN ('Terminado', 'Cancelado') THEN 'Por proyecto'
                WHEN c.Estado IN ('Terminado', 'Cancelado') THEN 'Por contrato'
                ELSE 'No aplica'
            END as razon_inclusion,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM proyectos p
        INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
        WHERE (p.Estado IN ('Terminado', 'Cancelado') 
               OR c.Estado IN ('Terminado', 'Cancelado'))
        ORDER BY p.ID_Proyecto
        """
        return self.execute_query(query, "proyectos")

    # ================= PLANIFICACIÓN Y EJECUCIÓN =================
    
    def extract_hitos(self) -> pd.DataFrame:
        """Extraer datos de hitos - SOLO de proyectos/contratos terminados o cancelados"""
        query = """
        SELECT 
            h.ID_Hito,
            h.ID_Proyecto,
            h.Descripcion,
            h.Estado,
            h.FechaInicio,
            h.FechaFinPlanificada,
            h.FechaFinReal,
            DATEDIFF(IFNULL(h.FechaFinReal, CURDATE()), h.FechaFinPlanificada) as dias_retraso,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM hitos h
        INNER JOIN proyectos p ON h.ID_Proyecto = p.ID_Proyecto
        INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
        WHERE (p.Estado IN ('Terminado', 'Cancelado') 
               OR c.Estado IN ('Terminado', 'Cancelado'))
        ORDER BY h.ID_Proyecto, h.ID_Hito
        """
        return self.execute_query(query, "hitos")
    
    def extract_tareas(self) -> pd.DataFrame:
        """Extraer datos de tareas - SOLO de proyectos/contratos terminados o cancelados"""
        query = """
        SELECT 
            t.ID_Tarea,
            t.ID_Hito,
            h.ID_Proyecto,
            t.NombreTarea,
            t.Descripcion,
            t.Estado,
            t.DuracionPlanificada,
            t.DuracionReal,
            IFNULL(t.DuracionReal, 0) - IFNULL(t.DuracionPlanificada, 0) as desviacion_duracion,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM tareas t
        INNER JOIN hitos h ON t.ID_Hito = h.ID_Hito
        INNER JOIN proyectos p ON h.ID_Proyecto = p.ID_Proyecto
        INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
        WHERE (p.Estado IN ('Terminado', 'Cancelado') 
               OR c.Estado IN ('Terminado', 'Cancelado'))
        ORDER BY h.ID_Proyecto, t.ID_Hito, t.ID_Tarea
        """
        return self.execute_query(query, "tareas")
    
    def extract_asignaciones(self) -> pd.DataFrame:
        """Extraer datos de asignaciones - SOLO de proyectos/contratos terminados o cancelados"""
        query = """
        SELECT 
            a.ID_Asignacion,
            a.ID_Proyecto,
            a.ID_Empleado,
            a.HorasPlanificadas,
            a.HorasReales,
            a.FechaAsignacion,
            e.CostoPorHora,
            (a.HorasReales * e.CostoPorHora) as costo_real_horas,
            (a.HorasPlanificadas * e.CostoPorHora) as costo_planificado_horas,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM asignaciones a
        INNER JOIN empleados e ON a.ID_Empleado = e.ID_Empleado
        INNER JOIN proyectos p ON a.ID_Proyecto = p.ID_Proyecto
        INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
        WHERE (p.Estado IN ('Terminado', 'Cancelado') 
               OR c.Estado IN ('Terminado', 'Cancelado'))
        ORDER BY a.ID_Proyecto, a.FechaAsignacion
        """
        return self.execute_query(query, "asignaciones")

    # ================= CALIDAD Y CONTROL =================
    
    def extract_pruebas(self) -> pd.DataFrame:
        """Extraer datos de pruebas - SOLO de proyectos/contratos terminados o cancelados"""
        query = """
        SELECT 
            pr.ID_Prueba,
            pr.ID_Hito,
            h.ID_Proyecto,
            pr.TipoPrueba,
            pr.Fecha,
            pr.Exitosa,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM pruebas pr
        INNER JOIN hitos h ON pr.ID_Hito = h.ID_Hito
        INNER JOIN proyectos p ON h.ID_Proyecto = p.ID_Proyecto
        INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
        WHERE (p.Estado IN ('Terminado', 'Cancelado') 
               OR c.Estado IN ('Terminado', 'Cancelado'))
        ORDER BY h.ID_Proyecto, pr.ID_Hito, pr.Fecha
        """
        return self.execute_query(query, "pruebas")
    
    def extract_errores(self) -> pd.DataFrame:
        """Extraer datos de errores - SOLO de proyectos/contratos terminados o cancelados"""
        query = """
        SELECT 
            e.ID_Error,
            e.ID_Tarea,
            t.ID_Hito,
            h.ID_Proyecto,
            e.TipoError,
            e.Descripcion,
            e.Fecha,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM errores e
        INNER JOIN tareas t ON e.ID_Tarea = t.ID_Tarea
        INNER JOIN hitos h ON t.ID_Hito = h.ID_Hito
        INNER JOIN proyectos p ON h.ID_Proyecto = p.ID_Proyecto
        INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
        WHERE (p.Estado IN ('Terminado', 'Cancelado') 
               OR c.Estado IN ('Terminado', 'Cancelado'))
        ORDER BY h.ID_Proyecto, e.Fecha
        """
        return self.execute_query(query, "errores")

    # ================= RIESGOS =================
    
    def extract_riesgos(self) -> pd.DataFrame:
        """Extraer datos de riesgos - SOLO de proyectos/contratos terminados o cancelados"""
        query = """
        SELECT 
            r.ID_Riesgo,
            r.ID_Proyecto,
            r.TipoRiesgo,
            r.Severidad,
            r.Descripcion,
            r.FechaRegistro,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM riesgos r
        INNER JOIN proyectos p ON r.ID_Proyecto = p.ID_Proyecto
        INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
        WHERE (p.Estado IN ('Terminado', 'Cancelado') 
               OR c.Estado IN ('Terminado', 'Cancelado'))
        ORDER BY r.ID_Proyecto, r.FechaRegistro
        """
        return self.execute_query(query, "riesgos")

    # ================= FINANZAS =================
    
    def extract_gastos(self) -> pd.DataFrame:
        """Extraer datos de gastos - SOLO de proyectos/contratos terminados o cancelados"""
        query = """
        SELECT 
            g.ID_Gasto,
            g.ID_Proyecto,
            g.TipoGasto,
            g.Categoria,
            g.Monto,
            g.Fecha,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM gastos g
        INNER JOIN proyectos p ON g.ID_Proyecto = p.ID_Proyecto
        INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
        WHERE (p.Estado IN ('Terminado', 'Cancelado') 
               OR c.Estado IN ('Terminado', 'Cancelado'))
        ORDER BY g.ID_Proyecto, g.Fecha
        """
        return self.execute_query(query, "gastos")
    
    def extract_penalizaciones(self) -> pd.DataFrame:
        """Extraer datos de penalizaciones - SOLO de contratos terminados o cancelados"""
        query = """
        SELECT 
            p.ID_Penalizacion,
            p.ID_Contrato,
            c.ID_Cliente,
            p.Monto,
            p.Motivo,
            p.Fecha,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM penalizaciones p
        INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
        WHERE c.Estado IN ('Terminado', 'Cancelado')
        ORDER BY p.ID_Contrato, p.Fecha
        """
        return self.execute_query(query, "penalizaciones")

    # ================= MÉTODO PRINCIPAL =================
    
    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """
        Extraer todos los datos siguiendo el orden de dependencias
        Retorna diccionario con DataFrames de todas las tablas
        """
        if not self.connect():
            return {}
        
        extracted_data = {}
        
        try:
            logger.info("=== INICIANDO EXTRACCIÓN DE DATOS SGP ===")
            
            # 1. Tablas Maestro
            logger.info("--- Extrayendo Tablas Maestro ---")
            extracted_data['clientes'] = self.extract_clientes()
            extracted_data['empleados'] = self.extract_empleados()
            extracted_data['contratos'] = self.extract_contratos()
            extracted_data['proyectos'] = self.extract_proyectos()
            
            # 2. Planificación y Ejecución
            logger.info("--- Extrayendo Planificación y Ejecución ---")
            extracted_data['hitos'] = self.extract_hitos()
            extracted_data['tareas'] = self.extract_tareas()
            extracted_data['asignaciones'] = self.extract_asignaciones()
            
            # 3. Calidad y Control
            logger.info("--- Extrayendo Calidad y Control ---")
            extracted_data['pruebas'] = self.extract_pruebas()
            extracted_data['errores'] = self.extract_errores()
            
            # 4. Riesgos
            logger.info("--- Extrayendo Riesgos ---")
            extracted_data['riesgos'] = self.extract_riesgos()
            
            # 5. Finanzas
            logger.info("--- Extrayendo Finanzas ---")
            extracted_data['gastos'] = self.extract_gastos()
            extracted_data['penalizaciones'] = self.extract_penalizaciones()
            
            logger.info("=== EXTRACCIÓN COMPLETADA EXITOSAMENTE ===")
            
            # Resumen de extracción
            total_records = sum(len(df) for df in extracted_data.values())
            logger.info(f"Total de registros extraídos: {total_records}")
            
            for table_name, df in extracted_data.items():
                logger.info(f"  - {table_name}: {len(df)} registros")
                
        except Exception as e:
            logger.error(f"Error durante la extracción: {str(e)}")
        finally:
            self.disconnect()
            
        return extracted_data


def extract_all() -> Dict[str, pd.DataFrame]:
    """Función principal para extraer todos los datos"""
    extractor = SGPExtractor()
    return extractor.extract_all()


if __name__ == "__main__":
    # Test de extracción
    data = extract_all()
    print("Extracción completada!")
    for table, df in data.items():
        print(f"{table}: {len(df)} registros")