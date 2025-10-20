DROP DATABASE IF EXISTS gestion_proyectos;
CREATE DATABASE gestion_proyectos;
USE gestion_proyectos;

-- =============== MAESTROS ===================

CREATE TABLE clientes (
  ID_Cliente INT PRIMARY KEY AUTO_INCREMENT,
  NombreCliente VARCHAR(255) NOT NULL
);

CREATE TABLE empleados (
  ID_Empleado INT PRIMARY KEY AUTO_INCREMENT,
  NombreCompleto VARCHAR(255) NOT NULL,
  Rol VARCHAR(100) NOT NULL,
  Seniority VARCHAR(100),
  CostoPorHora DECIMAL(12,2) DEFAULT 0
);

CREATE TABLE contratos (
  ID_Contrato INT PRIMARY KEY AUTO_INCREMENT,
  ID_Cliente INT NOT NULL,
  ValorTotalContrato DECIMAL(15,2) NOT NULL,
  Estado VARCHAR(50) DEFAULT 'Activo',
  FOREIGN KEY (ID_Cliente) REFERENCES clientes(ID_Cliente),
  INDEX idx_contrato_cliente (ID_Cliente)
);

CREATE TABLE proyectos (
  ID_Proyecto INT PRIMARY KEY AUTO_INCREMENT,
  ID_Contrato INT NOT NULL,
  NombreProyecto VARCHAR(255) NOT NULL,
  Version VARCHAR(50),
  FechaInicio DATE,
  FechaFin DATE,
  Estado VARCHAR(50),
  FOREIGN KEY (ID_Contrato) REFERENCES contratos(ID_Contrato),
  INDEX idx_proy_contrato (ID_Contrato),
  INDEX idx_proy_estado (Estado)
);

-- =============== PLANIFICACIÓN Y EJECUCIÓN ===============

CREATE TABLE hitos (
  ID_Hito INT PRIMARY KEY AUTO_INCREMENT,
  ID_Proyecto INT NOT NULL,
  Descripcion VARCHAR(255) NOT NULL,
  Estado VARCHAR(50),
  FechaInicio DATE,
  FechaFinPlanificada DATE,
  FechaFinReal DATE,
  FOREIGN KEY (ID_Proyecto) REFERENCES proyectos(ID_Proyecto),
  INDEX idx_hito_proy (ID_Proyecto)
);

CREATE TABLE tareas (
  ID_Tarea INT PRIMARY KEY AUTO_INCREMENT,
  ID_Hito INT NOT NULL,
  NombreTarea VARCHAR(100) NOT NULL,
  Descripcion TEXT,
  Estado VARCHAR(50),
  DuracionPlanificada INT,
  DuracionReal INT,
  FOREIGN KEY (ID_Hito) REFERENCES hitos(ID_Hito),
  INDEX idx_tarea_hito (ID_Hito)
);

CREATE TABLE asignaciones (
  ID_Asignacion INT PRIMARY KEY AUTO_INCREMENT,
  ID_Proyecto INT NOT NULL,
  ID_Empleado INT NOT NULL,
  HorasPlanificadas DECIMAL(10,2) DEFAULT 0,
  HorasReales DECIMAL(10,2) DEFAULT 0,
  FechaAsignacion DATE,
  FOREIGN KEY (ID_Proyecto) REFERENCES proyectos(ID_Proyecto),
  FOREIGN KEY (ID_Empleado) REFERENCES empleados(ID_Empleado),
  INDEX idx_asig_proyecto (ID_Proyecto),
  INDEX idx_asig_empleado (ID_Empleado),
  INDEX idx_asig_fecha (FechaAsignacion)
);

-- =============== CALIDAD ====================

CREATE TABLE pruebas (
  ID_Prueba INT PRIMARY KEY AUTO_INCREMENT,
  ID_Hito INT NOT NULL,
  TipoPrueba VARCHAR(50) NOT NULL,
  Fecha DATE,
  Exitosa TINYINT(1),
  FOREIGN KEY (ID_Hito) REFERENCES hitos(ID_Hito),
  INDEX idx_prueba_hito (ID_Hito)
);

CREATE TABLE errores (
  ID_Error INT PRIMARY KEY AUTO_INCREMENT,
  ID_Tarea INT NOT NULL,
  TipoError VARCHAR(50),
  Descripcion TEXT,
  Fecha DATE,
  FOREIGN KEY (ID_Tarea) REFERENCES tareas(ID_Tarea),
  INDEX idx_error_tarea (ID_Tarea)
);

-- =============== RIESGOS ====================

CREATE TABLE riesgos (
  ID_Riesgo INT PRIMARY KEY AUTO_INCREMENT,
  ID_Proyecto INT NOT NULL,
  TipoRiesgo VARCHAR(50),
  Severidad VARCHAR(50),
  Descripcion TEXT,
  FechaRegistro DATE,
  FOREIGN KEY (ID_Proyecto) REFERENCES proyectos(ID_Proyecto),
  INDEX idx_riesgo_proy (ID_Proyecto)
);

-- =============== FINANZAS ===================

CREATE TABLE gastos (
  ID_Gasto INT PRIMARY KEY AUTO_INCREMENT,
  ID_Proyecto INT NOT NULL,
  TipoGasto VARCHAR(50) NOT NULL,
  Categoria VARCHAR(20),
  Monto DECIMAL(15,2) NOT NULL,
  Fecha DATE NOT NULL,
  FOREIGN KEY (ID_Proyecto) REFERENCES proyectos(ID_Proyecto),
  INDEX idx_gasto_proy_fecha (ID_Proyecto, Fecha)
);

CREATE TABLE penalizaciones (
  ID_Penalizacion INT PRIMARY KEY AUTO_INCREMENT,
  ID_Contrato INT NOT NULL,
  Monto DECIMAL(15,2) NOT NULL,
  Motivo TEXT,
  Fecha DATE NOT NULL,
  FOREIGN KEY (ID_Contrato) REFERENCES contratos(ID_Contrato),
  INDEX idx_penal_contrato (ID_Contrato, Fecha)
);

-- =============== INDICES DE APOYO ===========

CREATE INDEX idx_hito_fechas ON hitos(FechaInicio, FechaFinPlanificada, FechaFinReal);
CREATE INDEX idx_tarea_estado ON tareas(Estado);
