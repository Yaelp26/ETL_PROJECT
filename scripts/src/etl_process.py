# src/etl_process.py
from mysql.connector import Error
from src.db_connector import get_db_connection
from src.transformations import transformar_datos_usuarios

def run_etl():
    """Función principal que orquesta el proceso ETL completo."""
    source_conn = get_db_connection('source_db')
    dest_conn = get_db_connection('destination_db')

    if not source_conn or not dest_conn:
        print("No se pudo establecer una de las conexiones. Abortando proceso.")
        return

    try:
        with source_conn.cursor() as source_cursor, \
             dest_conn.cursor() as dest_cursor:

            # 1. EXTRACT (Extraer)
            print("\n--- PASO 1: Extrayendo datos del origen ---")
            sql_select = "SELECT id, nombre, email, fecha_registro FROM usuarios WHERE procesado = 0"
            source_cursor.execute(sql_select)
            registros_crudos = source_cursor.fetchall()

            if not registros_crudos:
                print("No se encontraron nuevos registros para procesar.")
                return

            # 2. TRANSFORM (Transformar)
            print("\n--- PASO 2: Transformando datos ---")
            datos_transformados = transformar_datos_usuarios(registros_crudos)

            # 3. LOAD (Cargar)
            print("\n--- PASO 3: Cargando datos en el destino ---")
            sql_insert = "INSERT INTO usuarios_importados (origen_id, nombre_completo, correo, fecha_creacion) VALUES (%s, %s, %s, %s)"
            dest_cursor.executemany(sql_insert, datos_transformados)
            dest_conn.commit()
            print(f"✅ ¡Éxito! Se cargaron {dest_cursor.rowcount} registros en el destino.")

    except Error as e:
        print(f"❌ Ocurrió un error en el proceso ETL: {e}")
        if dest_conn:
            dest_conn.rollback() # Revertir cambios en caso de error

    finally:
        if source_conn and source_conn.is_connected():
            source_conn.close()
        if dest_conn and dest_conn.is_connected():
            dest_conn.close()
        print("\nProceso finalizado. Conexiones cerradas.")