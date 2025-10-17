# Plan de Implementación ETL - Basado en Mapeo de Datos

## 📊 **FASE 1: PREPARACIÓN DE CATÁLOGOS** ✅ (Ya completada parcialmente)

### Catálogos poblados:
- ✅ **dim_severidad**: 3 registros
- ✅ **subdim_anio**: 11 registros (2020-2030)
- ✅ **subdim_mes**: 12 registros (1-12)
- ✅ **subdim_dia**: 7 registros (1-7)
- ✅ **dim_finanzas**: 3 tipos básicos

### Pendientes por poblar:
- ⚠️ **dim_tipo_riesgo**: Verificar estructura de tabla `riesgos`
- 📋 **Tipos de gasto específicos**: De tabla `gastos`

## 📊 **FASE 2: MAPEO DETALLADO POR TABLA ORIGEN**

### 1. **TABLA ORIGEN: proyectos** → **DESTINOS MÚLTIPLES**
```sql
-- Mapeo principal
proyectos.ID_Proyecto → dim_proyectos.CodigoProyecto
proyectos.CostoPresupuestado → dim_proyectos.CostoPresupuestado  
proyectos.CostoReal → dim_proyectos.CostoReal
proyectos.Estado → dim_proyectos.Estado

-- Mapeo temporal (requiere procesamiento)
proyectos.FechaInicio → dim_tiempo.Fecha + referencias a subdimensiones
proyectos.FechaFin → dim_tiempo.Fecha + referencias a subdimensiones
```

### 2. **TABLA ORIGEN: clientes** → **dim_clientes**
```sql
clientes.ID_Cliente → dim_clientes.CodigoClienteReal
clientes.NombreCliente → dim_clientes.NombreCliente
```

### 3. **TABLA ORIGEN: empleados** → **dim_empleados**
```sql
empleados.ID_Empleado → dim_empleados.CodigoEmpleado
empleados.Rol → dim_empleados.Rol
empleados.CostoPorHora * HorasTrabajo → dim_empleados.Salario
```

### 4. **TABLA ORIGEN: tareas** → **dim_tareas**
```sql
tareas.ID_Tarea → dim_tareas.CodigoTarea
tareas.DuracionPlanificada → dim_tareas.DuracionPlanificadaWeek
tareas.DuracionReal → dim_tareas.DuracionRealWeek
tareas.ID_Proyecto → dim_tareas.dim_proyectos_ID_proyectos (FK)
```

### 5. **TABLA ORIGEN: gastos** → **dim_finanzas**
```sql
gastos.TipoGasto → dim_finanzas.TipoGasto (categorización)
gastos.Monto → dim_finanzas.Monto
```

### 6. **TABLA ORIGEN: penalizaciones** → **dim_finanzas**
```sql
penalizaciones.Monto → dim_finanzas.Monto
'Penalización' → dim_finanzas.TipoGasto (fijo)
```

### 7. **TABLA ORIGEN: hitos** → **dim_hitos**
```sql
hitos.ID_Hito → dim_hitos.CodigoHito
hitos.ID_Proyecto → dim_hitos.ID_proyectos (FK)
DATEDIFF(FechaFinReal, FechaInicioPlanificada) → dim_hitos.Retraso_days
```

### 8. **TABLA ORIGEN: riesgos** → **dim_riesgos**
```sql
riesgos.Tipo → dim_tipo_riesgo.NombreTipo (lookup)
riesgos.Severidad → dim_severidad.Nivel (lookup)
```

## 📊 **FASE 3: AGREGACIONES PARA TABLA DE HECHOS**

### **hechos_proyectos** - Métricas calculadas por proyecto:

#### **De asignaciones:**
```sql
-- Por proyecto
SUM(asignaciones.HorasEstimadas) → hechos_proyectos.HorasPlanificadas
SUM(asignaciones.HorasReales) → hechos_proyectos.HorasReales
```

#### **De errores:**
```sql
-- Por proyecto
COUNT(errores.ID_Error) → hechos_proyectos.NumeroDefectosEncontrados
```

#### **De pruebas:**
```sql
-- Por proyecto
COUNT(pruebas WHERE Exitoso = true) → hechos_proyectos.NumeroTestExitosos
```

#### **Cálculos derivados:**
```sql
-- Ganancias
contratos.ValorTotalContrato - proyectos.CostoReal → hechos_proyectos.Ganancias
```

## 🔄 **FASE 4: ORDEN DE CARGA (CRÍTICO)**

### **1. Dimensiones independientes:**
1. dim_clientes
2. dim_empleados 
3. dim_tipo_riesgo
4. dim_severidad
5. dim_finanzas (gastos + penalizaciones)

### **2. Dimensiones dependientes:**
6. dim_tiempo (fechas de proyectos)
7. dim_proyectos (requiere dim_clientes)
8. dim_tareas (requiere dim_proyectos)
9. dim_hitos (requiere dim_proyectos)
10. dim_riesgos (requiere dim_tipo_riesgo + dim_severidad)

### **3. Tablas de relación:**
11. dim_empleados_has_dim_proyectos

### **4. Tabla de hechos (al final):**
12. hechos_proyectos (requiere TODAS las dimensiones)

## ⚠️ **CONSIDERACIONES TÉCNICAS**

### **Joins complejos identificados:**
```sql
-- Para relacionar empleados con proyectos vía asignaciones
asignaciones.tareas_ID_Tarea = tareas.ID_Tarea
asignaciones.empleados_ID_Empleado = empleados.ID_Empleado
tareas.ID_Proyecto = proyectos.ID_Proyecto
```

### **Transformaciones necesarias:**
- Conversión de fechas a componentes (día, mes, año)
- Cálculos de retrasos (DATEDIFF)
- Agregaciones por proyecto (SUM, COUNT)
- Lookups en catálogos
- Cálculo de ganancias

### **Validaciones requeridas:**
- Verificar que todos los proyectos tengan fechas válidas
- Validar que los tipos de gasto existan en catálogo
- Asegurar integridad referencial en FKs
- Verificar que las agregaciones no sean NULL

## 🚀 **PRÓXIMO PASO SUGERIDO**
Implementar las funciones de transformación y carga en el orden especificado, comenzando por completar los catálogos pendientes.