# ETL Project - Sistema de Gestión de Proyectos a Data Warehouse

## Descripción General

Este proyecto implementa un proceso ETL (Extract, Transform, Load) completo para migrar datos desde un **Sistema de Gestión de Proyectos (SGP)** hacia un **Data Warehouse dimensional** optimizado para análisis de inteligencia de negocios.

### Objetivo
Transformar datos operacionales de gestión de proyectos en un modelo dimensional que permita:
- Análisis de rentabilidad por proyecto y cliente
- Métricas de productividad y calidad
- Seguimiento de riesgos y desviaciones
- Reporting ejecutivo y dashboards

---

##  Arquitectura del Sistema

```
SGP (OLTP)                    ETL Process                    DW (OLAP)
┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
│   gestion_      │   ────>  │    Extract      │   ────>  │   dw_proyectos  │
│   proyectos     │          │   Transform     │          │                 │
│                 │          │     Load        │          │  - Dimensiones  │
│ - clientes      │          │                 │          │  - Hechos       │
│ - proyectos     │          │ Filtro Crítico: │          │  - Métricas     │
│ - empleados     │          │ Solo proyectos/ │          │    Calculadas   │
│ - contratos     │          │ contratos       │          │                 │
│ - hitos         │          │ 'Terminado' o   │          │                 │
│ - tareas        │          │ 'Cancelado'     │          │                 │
│ - etc...        │          │                 │          │                 │
└─────────────────┘          └─────────────────┘          └─────────────────┘
```

---

##  Estructura del Proyecto

```
etl_project/
├── 📄 README.md                    # Este archivo
├── 📄 requirements.txt             # Dependencias Python
├── 📄 main_etl.py                 # Punto de entrada principal
│
├── 📁 config/                     # Configuraciones
│   ├── db_config.py               # Conexiones a BD
│   └── __pycache__/
│
├── 📁 DB/                         # Scripts de Base de Datos
│   ├── BD_SGP.sql                 # Creación BD Origen (OLTP)
│   └── DW_SSD.sql                 # Creación Data Warehouse (OLAP)
│
├── 📁 extract/                    # Módulo de Extracción
│   └── extract_gestion.py         # Extractor principal SGP
│
├── 📁 transform/                  # Módulo de Transformación
│   ├── transform_dim/             # Transformaciones dimensionales
│   └── transform_fact/            # Transformaciones de hechos
│
├── 📁 load/                       # Módulo de Carga
│   └── load_to_dw.py             # Carga al Data Warehouse
│
├── 📁 logs/                       # Logs del sistema
│   └── etl_log.txt               # Registro de ejecuciones
│
└── 📁 utils/                      # Utilidades
    ├── helpers.py                 # Funciones auxiliares
    └── __pycache__/
```

---

##  Configuración del Entorno

### 1. **Instalación de Dependencias**
```bash
pip install -r requirements.txt
```

### 2. **Configuración de Base de Datos**
Crear archivo `.env` en la raíz del proyecto:
```env
# Configuración OLTP (Sistema de Gestión)
OLTP_HOST=localhost
OLTP_USER=root
OLTP_PASS=tu_password
OLTP_DB=gestion_proyectos

# Configuración Data Warehouse
DW_HOST=localhost
DW_USER=root
DW_PASS=tu_password
DW_DB=dw_proyectos
```

---

##  Proceso ETL Detallado

###  **EXTRACT (Extracción)**

####  **Regla de Negocio Crítica**
**SOLO** se extraen datos de:
- Proyectos con estado `'Terminado'` o `'Cancelado'`
- Contratos con estado `'Terminado'` o `'Cancelado'`

####  **Orden de Extracción**
1. **Tablas Maestro**: `clientes`, `empleados`, `contratos`, `proyectos`
2. **Planificación**: `hitos`, `tareas`, `asignaciones`
3. **Calidad**: `pruebas`, `errores`
4. **Riesgos**: `riesgos`
5. **Finanzas**: `gastos`, `penalizaciones`

#### 💡 **Características Especiales**
-  Joins estratégicos para optimizar transformación
-  Cálculos pre-computados (retrasos, costos, desviaciones)
-  Filtros aplicados en cascada a todas las tablas relacionadas
-  Logging detallado del proceso

###  **TRANSFORM (Transformación)**

####  **Modelo Dimensional**
**Dimensiones**:
- `dim_tiempo` (con subdimensiones)
- `dim_clientes`
- `dim_proyectos`
- `dim_empleados`
- `dim_hitos`
- `dim_tareas`
- `dim_riesgos`
- `dim_finanzas`

**Hechos**:
- `hechos_proyectos` (métricas principales)
- `hechos_asignaciones` (recursos y tiempo)

####  **Métricas Calculadas**
- Desviación presupuestal
- Productividad promedio
- Porcentaje de tareas/hitos retrasados
- Proporción CAPEX/OPEX
- Valor real de horas trabajadas

###  **LOAD (Carga)**
- Carga incremental optimizada
- Validación de integridad referencial
- Manejo de duplicados
- Control de errores y rollback

---

##  Uso del Sistema

### **Ejecución Completa**
```python
# Desde la raíz del proyecto
python main_etl.py
```

### **Ejecución por Módulos**
```python
# Solo extracción
from extract.extract_gestion import extract_all
data = extract_all()

# Solo transformación
from transform.transform_dims import transform_dims
from transform.transform_hechos import transform_hechos
dim_data = transform_dims(raw_data)
fact_data = transform_hechos(raw_data, dim_data)

# Solo carga
from load.load_to_dw import load_all
load_all(dim_data, fact_data)
```

### **Validación de Conexiones**
```python
from utils.helpers import test_connection
test_connection()
```

---

## Modelo de Datos

### ** Sistema Origen (SGP)**
Base de datos normalizada con las siguientes entidades principales:
- **Maestros**: Clientes, Empleados, Contratos, Proyectos
- **Operación**: Hitos, Tareas, Asignaciones
- **Control**: Pruebas, Errores, Riesgos
- **Finanzas**: Gastos, Penalizaciones

### ** Data Warehouse (DW)**
Esquema dimensional optimizado para análisis:
- **Dimensiones conformadas** para consistencia
- **Hechos agregados** para performance
- **Métricas pre-calculadas** para agilidad
- **Subdimensiones** para drill-down

---

##  Casos de Uso de Análisis

### ** Análisis Financiero**
- Rentabilidad por proyecto/cliente
- Control presupuestal y desviaciones
- Análisis de penalizaciones
- ROI por tipo de proyecto

### **⏱ Análisis Temporal**
- Cumplimiento de cronogramas
- Identificación de cuellos de botella
- Tendencias de productividad
- Estacionalidad de proyectos

### ** Análisis de Recursos**
- Utilización de empleados
- Costo por hora efectivo
- Distribución de cargas de trabajo
- Performance por seniority/rol

### ** Análisis de Calidad**
- Tasa de defectos por proyecto
- Efectividad de pruebas
- Análisis de riesgos materializados
- Mejora continua de procesos

---

##  Tecnologías Utilizadas

- **Python 3.x**: Lenguaje principal
- **pandas**: Manipulación de datos
- **mysql-connector-python**: Conectividad BD
- **python-dotenv**: Gestión de configuración
- **MySQL**: Sistema de base de datos

---

## 🏁 Estado del Proyecto

- ✅ **Extracción**: Implementada con reglas de negocio
- 🚧 **Transformación**: En desarrollo
- ⏳ **Carga**: Pendiente
- ⏳ **Testing**: Pendiente
- ⏳ **Documentación técnica**: Pendiente

---

*Este README se actualiza continuamente conforme evoluciona el proyecto.*