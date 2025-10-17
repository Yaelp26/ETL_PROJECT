# Proyecto ETL - Data Warehouse de Gestión de Proyectos

Sistema ETL inteligente y reproducible para migrar datos de gestión de proyectos de OLTP (base transaccional) a OLAP (data warehouse) usando Python y MySQL.


##Resumen Ejecutivo

###Características Principales
- **ETL Inteligente**: Analiza automáticamente el estado y ejecuta solo lo necesario
- **100% Reproducible**: Puede ejecutarse múltiples veces sin problemas
- **Validación Automática**: Verifica integridad de datos en cada paso
- **Configuración por Variables de Entorno**: Migrado de config.ini a .env
- **Filtrado de Datos**: Solo proyectos cerrados/cancelados para análisis histórico

### Métricas del Data Warehouse
- **10,970+ registros** distribuidos en 15 tablas
- **$5,194,486.38** en ganancias totales analizadas
- **141 proyectos** cerrados/cancelados procesados
- **429 fechas únicas** desde 2018 hasta 2026
- **9 años** de datos históricos listos para BI

##  Arquitectura del Sistema

###Estructura del Proyecto
```
etl_project/
├── 📂 config/
│   └── .env                    #Configuración de BD (variables de entorno)
├── 📂 scripts/
│   ├── etl_main.py            #Orquestador principal inteligente
│   ├── etl_help.py            #Sistema de ayuda
│   ├── etl_status.py          #Verificación rápida de estado
│   └── 📂 src/
│       ├── etl_validator.py           #Validación de estado del DW
│       ├── etl_catalog_loader.py      #Carga de catálogos
│       ├── etl_complete_processor.py  #Procesamiento ETL completo
│       ├── db_connector.py            #Gestión de conexiones BD
│       └── extract_proyectos.py       #Extracción de datos filtrados
├── 📂 logs/                   #Archivos de log del sistema
└── 📂 temp/                   #Archivos temporales de procesamiento
