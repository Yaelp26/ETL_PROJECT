# transform/transform_dim package
from .dim_clientes import transform as dim_clientes
from .dim_proyectos import transform as dim_proyectos
from .dim_empleados import transform as dim_empleados
from .dim_tiempo import transform as dim_tiempo
from .dim_finanzas import transform as dim_finanzas
from .dim_tipo_riesgo import transform as dim_tipo_riesgo
from .dim_severidad import transform as dim_severidad
from .dim_riesgos import transform as dim_riesgos
from .dim_hitos import transform as dim_hitos
from .dim_tareas import transform as dim_tareas
from .dim_pruebas import transform as dim_pruebas

__all__ = [
    'dim_clientes','dim_proyectos','dim_empleados','dim_tiempo',
    'dim_finanzas','dim_tipo_riesgo','dim_severidad','dim_riesgos',
    'dim_hitos','dim_tareas','dim_pruebas'
]
