# Migración de config.ini a .env

## Cambios realizados

### 1. Archivo de configuración
- **Antes**: `config.ini` (formato INI)
- **Ahora**: `config/.env` (variables de entorno)

### 2. Librerías utilizadas
- **Agregado**: `python-dotenv` para cargar variables de entorno desde archivo .env
- **Removido**: `configparser` ya no es necesario

### 3. Cambios en `db_connector.py`

#### Antes:
```python
import configparser

def get_db_connection(db_section):
    config = configparser.ConfigParser()
    config.read('config.ini')
    db_config = dict(config.items(db_section))
```

#### Ahora:
```python
import os
from dotenv import load_dotenv

load_dotenv('config/.env')

def get_db_connection(db_type):
    if db_type.lower() == 'origen':
        db_config = {
            'host': os.getenv('ORIGEN_HOST'),
            'user': os.getenv('ORIGEN_USER'),
            'password': os.getenv('ORIGEN_PASS'),
            'database': os.getenv('ORIGEN_DB')
        }
    elif db_type.lower() == 'destino':
        # ...configuración similar
```

### 4. Parámetros actualizados

| Archivo | Función | Cambio |
|---------|---------|---------|
| `etl_process.py` | `run_etl()` | `'source_db'` → `'origen'` |
| `etl_process.py` | `run_etl()` | `'destination_db'` → `'destino'` |
| `extract_proyectos.py` | `extract_and_show_data()` | `'source_db'` → `'origen'` |

### 5. Estructura del archivo .env

```properties
# Base de datos origen (OLTP)
ORIGEN_HOST=192.168.100.133
ORIGEN_USER=user_etl_DB
ORIGEN_PASS=DB321
ORIGEN_DB=gestion_proyectos

# Base de datos destino (OLAP)
DESTINO_HOST=192.168.0.11
DESTINO_USER=etl_user
DESTINO_PASS=clave123
DESTINO_DB=soporte_decision

# Parámetros del proceso
LOG_PATH=./logs/etl.log
TEMP_PATH=./temp/
```

### 6. Ventajas de usar .env

1. **Seguridad mejorada**: Las variables de entorno son más seguras para credenciales
2. **Compatibilidad**: Estándar ampliamente usado en la industria
3. **Flexibilidad**: Fácil integración con contenedores Docker y despliegues
4. **Simplicidad**: Sintaxis más simple que los archivos INI
5. **Validación**: Mejor manejo de errores cuando faltan variables

### 7. Verificación

Ejecutar el script de prueba:
```bash
python test_connection.py
```

Este script verifica:
- Conexión a base de datos origen
- Conexión a base de datos destino  
- Manejo correcto de parámetros inválidos
- Carga correcta de variables de entorno