# src/transformations.py

def transformar_datos_usuarios(registros):
    """
    Aplica varias transformaciones a una lista de registros de usuarios.
    :param registros: Una lista de tuplas, donde cada tupla es una fila de la BD de origen.
    :return: Una lista de tuplas con los datos transformados.
    """
    registros_transformados = []
    print("Iniciando transformación de datos...")
    
    for fila in registros:
        # Asumiendo que la fila de origen es (id, nombre, email, fecha_registro)
        origen_id = fila[0]
        nombre_completo = fila[1]
        correo = fila[2]
        fecha_creacion = fila[3]

        # --- EJEMPLOS DE TRANSFORMACIONES ---
        # 1. Limpiar y estandarizar el nombre a tipo Título
        nombre_completo_limpio = nombre_completo.strip().title()
        
        # 2. Convertir el correo a minúsculas
        correo_limpio = correo.strip().lower()

        # 3. Puedes agregar más lógica: validar el email, cambiar formato de fecha, etc.

        # Añadir la fila transformada a la lista de resultados
        registros_transformados.append(
            (origen_id, nombre_completo_limpio, correo_limpio, fecha_creacion)
        )
        
    print(f"Se transformaron {len(registros_transformados)} registros.")
    return registros_transformados