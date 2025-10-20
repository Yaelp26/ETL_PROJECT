# main_etl.py
from extract.extract_gestion import extract_all
from transform.transform_dim import transform_dims
from transform.transform_hechos import transform_hechos
from load.load_to_dw import load_all

def run_etl():
    raw_data = extract_all()
    dim_data = transform_dims(raw_data)
    fact_data = transform_hechos(raw_data, dim_data)
    load_all(dim_data, fact_data)

if __name__ == "__main__":
    run_etl()
