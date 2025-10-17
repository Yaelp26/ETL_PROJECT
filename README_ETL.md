# 🚀 Sistema ETL Inteligente - Data Warehouse

Sistema ETL modular y reproducible para la migración de datos de gestión de proyectos desde OLTP hacia OLAP.

## ✨ Características Principales

- **🧠 Inteligente**: Analiza automáticamente el estado actual y ejecuta solo lo necesario
- **🔄 Reproducible**: Puede ejecutarse múltiples veces sin problemas
- **📊 Validación Automática**: Verifica catálogos, dimensiones y tabla de hechos
- **🛠️ Modular**: Código organizado en módulos especializados
- **📝 Logging Detallado**: Rastrea cada paso del proceso
- **⚙️ Configuración por Entorno**: Usa variables de entorno (.env)

## 🏗️ Arquitectura

```
etl_project/
├── config/
│   └── .env                    # Configuración de BD
├── scripts/
│   ├── etl_main.py            # 🎯 Orquestador principal
│   ├── etl_help.py            # 📖 Sistema de ayuda
│   └── src/
│       ├── etl_validator.py           # 🔍 Validación de estado
│       ├── etl_catalog_loader.py      # 📚 Carga de catálogos
│       ├── etl_complete_processor.py  # ⚙️ Procesamiento completo
│       ├── db_connector.py            # 🔌 Conexiones BD
│       └── extract_proyectos.py       # 📤 Extracción datos
└── logs/                      # 📋 Archivos de log
```

## 🚀 Uso Rápido

### Ejecución Normal (Recomendado)
```bash
python scripts/etl_main.py
```

### Reconstrucción Completa
```bash
python scripts/etl_main.py --rebuild
```

### Ver Ayuda
```bash
python scripts/etl_main.py --help
```

## 📊 Estados del Data Warehouse

El sistema reconoce automáticamente 4 estados posibles:

| Estado | Descripción | Acción |
|--------|-------------|--------|
| ✅ **COMPLETO** | Todo listo | No hace nada |
| 🔶 **NECESITA_HECHOS** | Solo falta tabla de hechos | Carga hechos únicamente |
| 🔷 **NECESITA_DIMS** | Faltan dimensiones y hechos | Carga dimensiones + hechos |
| 🔴 **NECESITA_TODO** | Data warehouse vacío | Proceso ETL completo |

## ⚙️ Configuración

### Archivo `.env`
```properties
# Base de datos origen (OLTP)
ORIGEN_HOST=192.168.100.133
ORIGEN_USER=etl_user_BD
ORIGEN_PASS=BD321
ORIGEN_DB=gestion_proyectos

# Base de datos destino (OLAP)
DESTINO_HOST=192.168.100.133
DESTINO_USER=etl_user_DW
DESTINO_PASS=DW321
DESTINO_DB=project_dw
```

## 📈 Métricas Típicas

Después de un ETL exitoso, esperarías ver:

- **63** clientes únicos
- **400** empleados activos  
- **141** proyectos cerrados/cancelados
- **4,970** tareas asociadas
- **429** fechas únicas (2018-2026)
- **3,956** registros financieros
- **810** tipos de riesgo catalogados

## 🎯 Funcionalidades del Sistema

### 🔍 Validación Automática
- Verifica la existencia y población de todas las tablas
- Valida la calidad de los datos (ej: ganancias > 0)
- Detecta inconsistencias antes de procesar

### 📚 Carga de Catálogos
- **Subdimensiones Tiempo**: años, meses, días
- **Tipos de Riesgo**: extrae de la fuente OLTP
- **Severidades**: catálogo predefinido

### 📊 Procesamiento de Dimensiones
- **Clientes**: filtrados por proyectos cerrados/cancelados
- **Empleados**: que trabajaron en proyectos relevantes
- **Proyectos**: solo estados "Cerrado" o "Cancelado"
- **Tareas**: asociadas a proyectos relevantes
- **Tiempo**: fechas únicas extraídas de proyectos
- **Finanzas**: cálculos de ganancias y costos

### 🎯 Tabla de Hechos
- Métricas agregadas por proyecto
- Cálculo de ganancias: `ValorContrato - CostoReal`
- Relaciones con todas las dimensiones
- Conteos de tareas y empleados por proyecto

## 🛠️ Desarrollo

### Estructura Modular

Cada módulo tiene una responsabilidad específica:

- **`ETLValidator`**: Analiza el estado actual del DW
- **`ETLCatalogLoader`**: Puebla catálogos base
- **`ETLCompleteProcessor`**: Ejecuta el proceso ETL completo
- **`ETLOrchestrator`**: Coordina todo el proceso

### Extensibilidad

Para agregar nuevas dimensiones:

1. Actualizar `ETLValidator` con la nueva tabla
2. Agregar método `load_dim_nueva()` en `ETLCompleteProcessor`
3. Incluir en el flujo de `load_dimensions()`

## 🔧 Mantenimiento

### Limpieza de Logs
```bash
rm -rf logs/*.log
```

### Verificación de Estado
```bash
python scripts/src/etl_validator.py
```

### Repoblación de Solo Catálogos
```bash
python scripts/src/etl_catalog_loader.py
```

## 📋 Solución de Problemas

### Error de Conexión
- Verificar variables en `.env`
- Confirmar acceso de red a las BDs
- Validar credenciales de usuario

### Tablas Vacías
- Ejecutar con `--rebuild` para limpieza completa
- Verificar filtros de datos en origen
- Confirmar estados de proyectos ('Cerrado', 'Cancelado')

### Errores de Columnas
- Verificar estructura de BD origen
- Actualizar queries en módulos según esquema actual

## 🎉 Resultado Final

Un data warehouse completamente funcional con:

- **10,970+ registros** distribuidos en 15 tablas
- **Esquema estrella** optimizado para OLAP
- **Ganancias totales**: ~$5.2M en 141 proyectos
- **Análisis temporal**: 9 años de datos históricos
- **Listo para BI**: Compatible con herramientas de análisis

---

## 📞 Soporte

Para problemas o mejoras, consultar la documentación interna o usar:
```bash
python scripts/etl_main.py --help
```