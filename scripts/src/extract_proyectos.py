# src/extract_proyectos.py
from mysql.connector import Error
from src.db_connector import get_db_connection

def extract_and_show_data():
    """
    Se conecta a la base de datos de origen, extrae los datos de los proyectos
    y los muestra en la consola para verificación.
    """
    print("Iniciando proceso de extracción...")
    source_conn = get_db_connection('origen')

    if not source_conn:
        print("No se pudo establecer la conexión. Abortando.")
        return

    try:
        # Usamos un cursor para ejecutar la consulta (mejor práctica con context manager)
        with source_conn.cursor() as cursor:
            
            # --- LA CONSULTA DE EXTRACCIÓN (LA "E" DE ETL) ---
            sql_select = """
            SELECT
                p.ID_Proyecto,
                p.NombreProyecto,
                cli.NombreCliente,
                c.ValorTotalContrato,
                p.CostoPresupuestado,
                (SELECT SUM(a.HorasReales * e.CostoPorHora)
                   FROM asignaciones a
                   JOIN empleados e ON a.empleados_ID_Empleado = e.ID_Empleado
                   JOIN tareas t ON a.tareas_ID_Tarea = t.ID_Tarea
                   WHERE t.ID_Proyecto = p.ID_Proyecto) AS CostoTotalHoras,
                (SELECT SUM(g.Monto)
                   FROM gastos g
                   WHERE g.ID_Proyecto = p.ID_Proyecto) AS CostoTotalGastos,
                (SELECT SUM(pen.Monto)
                   FROM penalizaciones pen
                   WHERE pen.ID_Contrato = c.ID_Contrato) AS CostoTotalPenalizaciones
            FROM proyectos p
            LEFT JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
            LEFT JOIN clientes cli ON c.ID_Cliente = cli.ID_Cliente;
            """
            
            print("\nEjecutando consulta en la base de datos de origen...")
            cursor.execute(sql_select)
            
            # Obtenemos todos los registros encontrados
            registros = cursor.fetchall()

            if not registros:
                print("No se encontraron registros con la consulta.")
                return

            print(f"✅ ¡Extracción exitosa! Se encontraron {len(registros)} registros.")
            print("--- Mostrando los primeros 5 registros ---")

            # Imprimimos los encabezados para que sea más fácil de leer
            print("\nID_Proyecto | NombreProyecto | Cliente | ValorContrato | Presupuesto | CostoHoras | CostoGastos | CostoPenalizaciones")
            print("-" * 120)

            # Mostramos solo los primeros 5 para no saturar la pantalla
            for fila in registros[:5]:
                print(fila)

    except Error as e:
        print(f"❌ Ocurrió un error durante la extracción: {e}")

    finally:
        # Cerramos la conexión sin importar lo que pase
        if source_conn and source_conn.is_connected():
            source_conn.close()
            print("\nConexión de origen cerrada.")
