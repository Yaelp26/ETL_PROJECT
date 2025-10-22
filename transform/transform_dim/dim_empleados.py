"""
Transformación dim_empleados - Proyecto Escolar ETL
"""
import pandas as pd
import logging
from typing import Dict
import sys
import os

# Agregar path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from transform.common import ensure_df, log_transform_info

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['empleados', 'asignaciones']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Transformación para dim_empleados"""
    empleados = ensure_df(df_dict.get('empleados', pd.DataFrame()))
    
    if empleados.empty:
        logger.warning('dim_empleados: No hay datos de empleados')
        return pd.DataFrame(columns=['ID_Empleado','CodigoEmpleado','NombreCompleto','Rol','Seniority'])

    df = empleados.copy()
    df['CodigoEmpleado'] = df['ID_Empleado']
    
    # Limpiar datos
    df['NombreCompleto'] = df['NombreCompleto'].astype(str).str.strip()
    df['Rol'] = df['Rol'].astype(str).str.strip()
    df['Seniority'] = df['Seniority'].fillna('No especificado').astype(str).str.strip()

    result = df[['ID_Empleado','CodigoEmpleado','NombreCompleto','Rol','Seniority']]
    log_transform_info('dim_empleados', len(empleados), len(result))
    return result

def test_transform():
    sample_data = {
        'empleados': pd.DataFrame({
            'ID_Empleado': [1, 2, 3],
            'NombreCompleto': ['Juan Pérez', 'Ana García', 'Carlos López'],
            'Rol': ['Developer', 'Analyst', 'Manager'],
            'Seniority': ['Senior', 'Junior', 'Lead']
        })
    }
    result = transform(sample_data)
    print("Test dim_empleados:")
    print(result)
    return result

if __name__ == "__main__":
    test_transform()
