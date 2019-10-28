# import packages
import pandas as pd
from pandasql import PandaSQL

def preprocess_pcmh():
    pdsql = PandaSQL()

    # import datasets
    df_pcmh = pd.read_csv('input_datasets/datamart_pcmh.csv')
    
    # drop pcmh duplicate npi, keep most up-to-date status
    df_pcmh = pdsql("SELECT * FROM df_pcmh ORDER BY ncqa_std_yr DESC, certification_level DESC")
    df_pcmh = df_pcmh.drop_duplicates(subset=['npi'], keep='first')
    
    # export csv
    df_pcmh.to_csv('intermediate_datasets/preprocessed_pcmh.csv')
