# src/db_connector.py
import mysql.connector
import os
from dotenv import load_dotenv
from mysql.connector import Error
from pathlib import Path

# Obtener la ruta del directorio del proyecto (dos niveles arriba desde este archivo)
project_root = Path(__file__).parent.parent.parent
env_path = project_root / 'config' / '.env'

# Cargar variables del archivo .env
load_dotenv(env_path)

def get_db_connection(db_type):
    """
    Lee las variables de entorno y crea una conexión a la base de datos.
    :param db_type: El tipo de base de datos ('origen' o 'destino').
    :return: Un objeto de conexión o None si falla.
    """
    try:
        if db_type.lower() == 'origen':
            db_config = {
                'host': os.getenv('ORIGEN_HOST'),
                'user': os.getenv('ORIGEN_USER'),
                'password': os.getenv('ORIGEN_PASS'),
                'database': os.getenv('ORIGEN_DB')
            }
        elif db_type.lower() == 'destino':
            db_config = {
                'host': os.getenv('DESTINO_HOST'),
                'user': os.getenv('DESTINO_USER'),
                'password': os.getenv('DESTINO_PASS'),
                'database': os.getenv('DESTINO_DB')
            }
        else:
            raise ValueError(f"Tipo de base de datos no válido: {db_type}. Use 'origen' o 'destino'.")
        
        # Verificar que todas las variables estén definidas
        if not all(db_config.values()):
            missing_vars = [key for key, value in db_config.items() if not value]
            raise ValueError(f"Variables de entorno faltantes: {missing_vars}")
        
        connection = mysql.connector.connect(**db_config)
        
        if connection.is_connected():
            print(f" Conexión exitosa a la base de datos '{db_type}'.")
            return connection
            
    except Error as e:
        print(f" Error al conectar a la base de datos '{db_type}': {e}")
        return None
    except ValueError as e:
        print(f" Error de configuración: {e}")
        return None