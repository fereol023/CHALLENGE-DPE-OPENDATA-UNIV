import re
import numpy as np, pandas as pd
from functools import lru_cache
from unidecode import unidecode
from datetime import datetime


def get_entropy(pk, L):
    # pk : probabilité de chaque valeur
    # L : nombre de valeurs possibles
    # d : dataframe
    # cols : liste des colonnes à traiter
    op = - (pk * np.log(pk) / np.log(L)).sum()
    return op
 

def compute_entropies(d, cols):
    print('Computing entropies..')
    entropies = []
    for i,col in enumerate(cols):
        pk = d[col].value_counts(normalize=True, dropna = False).values
        entropy = round(get_entropy(pk, len(d)),2)
        entropies.append({"col": col, "entropy": entropy})
    return entropies      


@lru_cache(maxsize=1024)
def normalize_name(colname):
    pat1, pat2 = re.compile('[^0-9a-zA-Z]+'), re.compile('_+')
    return pat1.sub('_', pat2.sub('_', colname))


def normalize_df_colnames(df):
    return df.rename(columns={c: normalize_name(unidecode(c)).lower() for c in df.columns})


def get_today_date():
    return datetime.today().strftime('%Y_%m_%d')

def load_parquet_data(_PATH):
    print(f"Loading parquet data from : {_PATH}..")
    return pd.read_parquet(_PATH)