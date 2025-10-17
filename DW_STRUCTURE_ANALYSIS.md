# Estructura del Data Warehouse - project_dw

## 📊 Información General
- **Base de datos**: `project_dw`
- **Motor**: MySQL 8.0.43
- **Estado**: ✅ Conexión exitosa
- **Tablas totales**: 15

## 🏗️ Arquitectura del Data Warehouse

### 📋 **TABLAS DE DIMENSIONES (9 tablas)**

#### 1. **dim_clientes**
```sql
ID_Cliente (PK, AUTO_INCREMENT)
CodigoClienteReal (INT) - Referencia al ID original
NombreCliente (VARCHAR(255))
```

#### 2. **dim_empleados** 
```sql
ID_Empleado (PK, AUTO_INCREMENT)
CodigoEmpleado (INT) - Referencia al ID original
NombreEmpleado (VARCHAR(255))
Rol (VARCHAR(100))
Salario (FLOAT)
```

#### 3. **dim_proyectos**
```sql
ID_proyectos (PK, AUTO_INCREMENT)
CodigoProyecto (VARCHAR(50)) - Referencia al ID original
Estado (VARCHAR(50))
CostoPresupuestado (FLOAT)
CostoReal (FLOAT)
ID_Cliente (FK) → dim_clientes
```

#### 4. **dim_tareas**
```sql
ID_Tarea (PK, AUTO_INCREMENT)
CodigoTarea (INT) - Referencia al ID original
DuracionPlanificadaWeek (INT) - En semanas
DuracionRealWeek (INT) - En semanas
dim_proyectos_ID_proyectos (FK) → dim_proyectos
```

#### 5. **dim_finanzas**
```sql
ID_Finanza (PK, AUTO_INCREMENT)
TipoGasto (VARCHAR(50))
Monto (DECIMAL(15,2))
```

#### 6. **dim_riesgos**
```sql
ID_Riesgo (PK, AUTO_INCREMENT)
ID_TipoRiesgo (FK) → dim_tipo_riesgo
ID_Severidad (FK) → dim_severidad
```

#### 7. **dim_hitos**
```sql
ID_Hito (PK, AUTO_INCREMENT)
CodigoHito (VARCHAR(50))
ID_proyectos (FK) → dim_proyectos
Retraso_days (INT) - Retraso en días
```

#### 8. **dim_tiempo**
```sql
ID_Tiempo (PK, AUTO_INCREMENT)
Fecha (DATE)
ID_Dia (FK) → subdim_dia
ID_Mes (FK) → subdim_mes
ID_Anio (FK) → subdim_anio
```

#### 9. **dim_empleados_has_dim_proyectos** (Tabla de relación)
```sql
ID_Empleado (PK, FK) → dim_empleados
ID_proyectos (PK, FK) → dim_proyectos
```

### 📊 **TABLA DE HECHOS (1 tabla)**

#### **hechos_proyectos** - Tabla central del modelo estrella
```sql
ID_Hecho (PK, AUTO_INCREMENT, BIGINT)
ID_proyecto (FK) → dim_proyectos
ID_TiempoInicio (FK) → dim_tiempo
ID_TiempoFin (FK) → dim_tiempo
ID_Riesgo (FK) → dim_riesgos
ID_Finanza (FK) → dim_finanzas

-- MÉTRICAS/MEDIDAS
HorasPlanificadas (DECIMAL(10,2))
HorasReales (DECIMAL(10,2))
CostoPresupuestado (DECIMAL(15,2))
CostoReal (DECIMAL(15,2))
NumeroTestExitosos (INT)
NumeroDefectosEncontrados (INT)
Ganancias (DECIMAL(15,2))
```

### 📅 **SUB-DIMENSIONES DE TIEMPO (3 tablas)**

#### **subdim_anio**
```sql
ID_Anio (PK, AUTO_INCREMENT)
NumeroAnio (INT)
```

#### **subdim_mes**
```sql
ID_Mes (PK, AUTO_INCREMENT)
NumeroMes (INT)
```

#### **subdim_dia**
```sql
ID_Dia (PK, AUTO_INCREMENT)
NumeroDiaSemana (INT)
```

### 🔗 **DIMENSIONES DE CATÁLOGO**

#### **dim_tipo_riesgo**
```sql
ID_TipoRiesgo (PK, AUTO_INCREMENT)
NombreTipo (VARCHAR(50))
```

#### **dim_severidad**
```sql
ID_Severidad (PK, AUTO_INCREMENT)
Nivel (VARCHAR(50))
```

## 🎯 **Modelo de Datos - Esquema Estrella**

```
                    dim_tiempo
                        |
                        |
dim_clientes ← dim_proyectos → hechos_proyectos ← dim_riesgos
                        ↑              ↓
                   dim_tareas    dim_finanzas
                        |
                 dim_empleados
```

## 📈 **Estado Actual**
- ✅ **Todas las tablas creadas**
- ⚠️ **Sin datos** (0 registros en todas las tablas)
- 🎯 **Listo para proceso ETL**

## 🔄 **Proceso ETL Sugerido**

### **EXTRACT** (Ya implementado)
- ✅ Filtro: Proyectos y contratos cerrados/cancelados
- ✅ Datos origen: 141 registros de calidad

### **TRANSFORM** (Por implementar)
- Mapeo de IDs originales a códigos
- Conversión de duraciones a semanas
- Cálculo de ganancias (ValorContrato - CostoReal)
- Agregación de métricas por proyecto

### **LOAD** (Por implementar)
- Carga secuencial: Dimensiones → Hechos
- Manejo de claves foráneas
- Validación de integridad referencial