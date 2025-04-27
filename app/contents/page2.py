from contents import *



def main():
    df = load_parquet_data('ressources/data/2_intermediary/enedis_ban_ademe_extract_PARIS_2022.parquet').head(10)
    
    st.title('Analyse Exploratoire des Données')

    st.header('Présentation générale des données')
    st.markdown('''
    Le jeu de données utilisé dans cette étude provient de la base de données des DPE (Diagnostic de Performance Énergétique)
    de la région Île-de-France, couvrant la période de 2010 à 2022.
    Ce jeu de données contient des informations sur les logements, y compris leur consommation d'énergie,
    leur performance énergétique et d'autres caractéristiques pertinentes.
    ''')
    st.header('Aperçu des données')
    st.dataframe(df)
    

    st.header('la répartion des DPE')
    st.header('la repartion de la consommation réelle vs estimée')
    st.header('la repartion de la consommation réelle vs estimée par DPE')
    st.header('la distribution de la consommation')
    st.header('la distribution de la consommation par DPE')
    st.header('la distribution de la consommation par type de logement')
    st.header('info sur la déperdition')
    st.header('écart type entre la consommation mesurée et la consommation prédite par le DPE, pour différents types de logements')
