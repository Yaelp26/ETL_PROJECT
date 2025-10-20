-- Creación del esquema DW
DROP DATABASE dw_proyectos;
CREATE DATABASE dw_proyectos;
USE dw_proyectos;

-- ======== DIMENSIÓN TIEMPO (con subdimensiones) ========

CREATE TABLE subdim_dia_semana (
    ID_Dia_Semana INT PRIMARY KEY AUTO_INCREMENT,
    NumeroDia INT
);

CREATE TABLE subdim_mes (
    ID_Mes INT PRIMARY KEY AUTO_INCREMENT,
    NumeroMes INT
);

CREATE TABLE subdim_anio (
    ID_Anio INT PRIMARY KEY AUTO_INCREMENT,
    NumeroAnio INT
);

CREATE TABLE dim_tiempo (
    ID_Tiempo INT PRIMARY KEY AUTO_INCREMENT,
    Fecha DATE,
    ID_DiaSemana INT,
    ID_Mes INT,
    ID_Anio INT,
    FOREIGN KEY (ID_DiaSemana) REFERENCES subdim_dia_semana(ID_Dia_Semana),
    FOREIGN KEY (ID_Mes) REFERENCES subdim_mes(ID_Mes),
    FOREIGN KEY (ID_Anio) REFERENCES subdim_anio(ID_Anio)
);

-- ================= DIMENSIONES =================

CREATE TABLE dim_clientes (
    ID_Cliente INT PRIMARY KEY AUTO_INCREMENT,
    CodigoClienteReal INT NOT NULL        -- ID original del sistema de gestion
);

CREATE TABLE dim_proyectos (
    ID_Proyecto INT PRIMARY KEY AUTO_INCREMENT,
    CodigoProyecto INT NOT NULL,        -- ID original del sistema de gestion
    Version VARCHAR(50),
    Cancelado TINYINT(1) DEFAULT 0,
    ID_Cliente INT,
    FOREIGN KEY (ID_Cliente) REFERENCES dim_clientes(ID_Cliente)
);

CREATE TABLE dim_empleados (
    ID_Empleado INT PRIMARY KEY AUTO_INCREMENT,
    CodigoEmpleado INT NOT NULL,        -- ID original del sistema de gestion
    Rol VARCHAR(100),
    Seniority VARCHAR(100)
);

-- Hechos de asignaciones (empleado <-> proyecto <-> tiempo)
CREATE TABLE hechos_asignaciones (
    ID_HechoAsignacion BIGINT PRIMARY KEY AUTO_INCREMENT,
    ID_Empleado INT NOT NULL,
    ID_Proyecto INT NOT NULL,
    ID_FechaAsignacion INT NOT NULL,                -- Fecha de asignacion
    HorasPlanificadas DECIMAL(10,2) DEFAULT 0,
    HorasReales DECIMAL(10,2) DEFAULT 0,
    ValorHoras DECIMAL(15,2),              -- Se calculara en el ETl (Horas asignadas x SalarioHora)
    FOREIGN KEY (ID_Empleado) REFERENCES dim_empleados(ID_Empleado),
    FOREIGN KEY (ID_Proyecto) REFERENCES dim_proyectos(ID_Proyecto),
    FOREIGN KEY (ID_FechaAsignacion) REFERENCES dim_tiempo(ID_Tiempo)
);

CREATE TABLE dim_finanzas (
    ID_Finanza INT PRIMARY KEY AUTO_INCREMENT,
    TipoGasto VARCHAR(50) NOT NULL,   --  "Materiales", "Penalizacion", "Salarios"
    Categoria VARCHAR(20),             --  "CAPEX" o "OPEX"
    Monto DECIMAL(15,2) NOT NULL
);

CREATE TABLE dim_tipo_riesgo (
    ID_TipoRiesgo INT PRIMARY KEY AUTO_INCREMENT,
    NombreTipo VARCHAR(50) NOT NULL   -- Ej. 'Tecnico', 'Operativo', 'Financiero'
);

CREATE TABLE dim_severidad (
    ID_Severidad INT PRIMARY KEY AUTO_INCREMENT,
    Nivel VARCHAR(50) NOT NULL        -- Ej. 'Critica', 'Alta', 'Media', 'Baja'
);

CREATE TABLE dim_riesgos (
    ID_Riesgo INT PRIMARY KEY AUTO_INCREMENT,
    ID_TipoRiesgo INT,
    ID_Severidad INT,
    FOREIGN KEY (ID_TipoRiesgo) REFERENCES dim_tipo_riesgo(ID_TipoRiesgo),
    FOREIGN KEY (ID_Severidad) REFERENCES dim_severidad(ID_Severidad)
);

CREATE TABLE dim_hitos (
    ID_Hito INT PRIMARY KEY AUTO_INCREMENT,
    CodigoHito INT NOT NULL,        -- ID original del sistema de gestion
    ID_proyectos INT,
    ID_FechaInicio INT,
    ID_FechaFinalizacion INT,
    Retraso_days INT,
    FOREIGN KEY (ID_proyectos) REFERENCES dim_proyectos(ID_Proyecto),
    FOREIGN KEY (ID_FechaInicio) REFERENCES dim_tiempo(ID_Tiempo),
    FOREIGN KEY (ID_FechaFinalizacion) REFERENCES dim_tiempo(ID_Tiempo)
);

CREATE TABLE dim_tareas (
    ID_Tarea INT PRIMARY KEY AUTO_INCREMENT,
    CodigoTarea INT NOT NULL,
    ID_Hito INT NOT NULL,      -- cada tarea pertenece a un hito
    DuracionDias INT,          -- duracion total de la tarea
    RetrasoDias INT DEFAULT 0, -- calculado en el ETL
    FOREIGN KEY (ID_Hito) REFERENCES dim_hitos(ID_Hito)
);

CREATE TABLE dim_pruebas (
    ID_Prueba INT PRIMARY KEY AUTO_INCREMENT,
    CodigoPrueba INT NOT NULL,        -- ID original del sistema de gestion
    ID_Hito INT NOT NULL,            -- cada prueba pertenece a un hito
    TipoPrueba VARCHAR(50) NOT NULL, -- Ej. 'Unitarias', 'Integracion', 'Aceptacion'
    PruebaExitosa TINYINT(1) DEFAULT NULL,  -- Null podemos interpretarlo como pruebas sin resultados o sin concluir
    FOREIGN KEY (ID_Hito) REFERENCES dim_hitos(ID_Hito)
);

-- ================= TABLA DE HECHOS =================

CREATE TABLE hechos_proyectos (
    ID_Hecho BIGINT PRIMARY KEY AUTO_INCREMENT,

    -- Relaciones
    ID_Proyecto INT NOT NULL,
    ID_TiempoInicio INT,
    ID_TiempoFinalizacion INT,
    ID_Riesgo INT,
    ID_Finanza INT,

    -- Metricas de tiempo
    DuracionRealDias INT DEFAULT 0,
    RetrasoDias INT DEFAULT 0,

    -- Metricas financieras
    PresupuestoCliente DECIMAL(15,2) NOT NULL,   -- monto pactado con el cliente
    CosteReal DECIMAL(15,2) DEFAULT 0.00,        -- suma total de gastos reales
    DesviacionPresupuestal DECIMAL(15,2) DEFAULT 0.00, -- CosteReal - PresupuestoCliente
    PenalizacionesMonto DECIMAL(15,2) DEFAULT 0.00,    -- sumatoria de penalizaciones (Tipo de gato == Penalizaciones)
    ProporcionCAPEX_OPEX DECIMAL(10,2) DEFAULT NULL,    -- InversionCAPEX / GastoOPEX

    -- Metricas de calidad y desempeno
    NumeroDefectosEncontrados INT DEFAULT 0,            -- Proviene del sistema de gestion unicamente
    ProductividadPromedio DECIMAL(10,2) DEFAULT NULL,   -- DuracionReal / NumEmpleados
    PorcentajeTareasRetrasadas DECIMAL(5,2) DEFAULT 0.00, -- %
    PorcentajeHitosRetrasados DECIMAL(5,2) DEFAULT 0.00,  -- %

    -- Claves foraneas
    FOREIGN KEY (ID_Proyecto) REFERENCES dim_proyectos(ID_Proyecto),
    FOREIGN KEY (ID_TiempoInicio) REFERENCES dim_tiempo(ID_Tiempo),
    FOREIGN KEY (ID_TiempoFinalizacion) REFERENCES dim_tiempo(ID_Tiempo),
    FOREIGN KEY (ID_Riesgo) REFERENCES dim_riesgos(ID_Riesgo),
    FOREIGN KEY (ID_Finanza) REFERENCES dim_finanzas(ID_Finanza)
);