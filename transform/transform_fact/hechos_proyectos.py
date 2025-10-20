import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['proyectos','hitos','tareas','gastos','penalizaciones','riesgos']

def transform(df_dict: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    """Transformación para hechos_proyectos
    Output sugerido (esqueleto): varias métricas agregadas por proyecto
    """
    proyectos = df_dict.get('proyectos', pd.DataFrame())
    if proyectos.empty:
        return pd.DataFrame()

    # Esqueleto: devolver columnas base
    cols = ['ID_Proyecto','DuracionRealDias','RetrasoDias','PresupuestoCliente','CosteReal','DesviacionPresupuestal','PenalizacionesMonto']
    logger.info('hechos_proyectos: esqueleto creado')
    return pd.DataFrame(columns=cols)
