import pandas as pd
import logging

logger = logging.getLogger(__name__)

def ensure_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    return df

def empty_df_with_columns(columns):
    return pd.DataFrame(columns=columns)
