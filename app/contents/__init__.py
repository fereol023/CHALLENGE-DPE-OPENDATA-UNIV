import json
import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

import sys, os
path = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.join(path, '../..'))

from utils.fonctions import load_parquet_data

def fonction_communes_pages():
    pass