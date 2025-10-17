1. Instalar la libreria pip install python-dotenv.
2. Crear la carpeta y el archivo ETL_PROJECT/config/.env.
3. Dentro del archivo ingresar los siguientes datos:

//Base de datos origen 
ORIGEN_HOST=0.0.0.0
ORIGEN_USER=user
ORIGEN_PASS=password
ORIGEN_DB=database

//Base de datos destino 
ORIGEN_HOST=0.0.0.0
ORIGEN_USER=user
ORIGEN_PASS=password
ORIGEN_DB=database

//Parámetros del proceso
LOG_PATH=./logs/etl.log
TEMP_PATH=./temp/

4. Ejecutar el archivo main.py.

