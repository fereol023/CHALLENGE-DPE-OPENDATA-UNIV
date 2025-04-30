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

@st.cache_resource
def load_image(image_path):
    try:
        with open(image_path, "rb") as image_file:
            return image_file.read()
    except FileNotFoundError:
        st.error(f"Image not found: {image_path}")
        return None