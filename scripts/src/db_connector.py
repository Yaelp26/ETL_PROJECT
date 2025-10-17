# src/db_connector.py
import mysql.connector
import configparser
from mysql.connector import Error

def get_db_connection(db_section):
    """
    Lee el archivo de configuración y crea una conexión a la base de datos.
    :param db_section: El nombre de la sección en config.ini ('source_db' o 'destination_db').
    :return: Un objeto de conexión o None si falla.
    """
    try:
        config = configparser.ConfigParser()
        config.read('config.ini')
        
        db_config = dict(config.items(db_section))
        connection = mysql.connector.connect(**db_config)
        
        if connection.is_connected():
            print(f" Conexión exitosa a '{db_section}'.")
            return connection
            
    except Error as e:
        print(f" Error al conectar a '{db_section}': {e}")
        return None