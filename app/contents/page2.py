from contents import *
from PIL import Image

def main(selected_ville, selected_annee):
    df = load_parquet_data('ressources/data/2_intermediary/enedis_ban_ademe_extract_PARIS_2022.parquet').head(10)
    
    st.title('Analyse Exploratoire des Données')

    st.header('Présentation générale des données')
    st.markdown('''
    Le jeu de données utilisé dans cette étude provient de la base de données des DPE (Diagnostic de Performance Énergétique)
    de la région Île-de-France, couvrant la période de 2010 à 2022.
    Ce jeu de données contient des informations sur les logements, y compris leur consommation d'énergie,
    leur performance énergétique et d'autres caractéristiques pertinentes.
    ''')

    st.header("Structure du dataset")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Avantages du dataset")
        st.markdown('''
- **Complétude**  
- **Granularité**  
- **Diversité des variables**  
- **Limite** : 10M de lignes de données.
''')

    with col2:
        st.subheader("Variables principales")
        st.markdown('''
- **Informations générales sur le logement**  
- **Localisation**  
- **Énergie et consommations** : 5 usages énergétiques (chauffage, eau chaude sanitaire, éclairage)  
- **Installation et équipements**
''')


    st.header('Aperçu des données')
    st.dataframe(df)
    
    st.header('La répartition des DPE par type de logement')

    # Charger et redimensionner les images
    img1 = Image.open('ressources/img/distribution_log_dpe_appart_tharse.png').resize((600, 500))
    img2 = Image.open('ressources/img/distribution_log_dpe_immeuble_tharse.png').resize((600, 500))
    img3 = Image.open('ressources/img/distribution_log_dpe_maison_tharse.png').resize((600, 500))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.image(img1, caption='Appartements')
    with col2:
        st.image(img2, caption='Immeubles')
    with col3:
        st.image(img3, caption='Maisons')

    st.markdown('''
    Il y a beaucoup plus d'appartements dans notre dataset que de maisons ou d'immeubles.
    La majorité des logements sont classés en classe D ou E.
    Alors que les classes A et B sont très rares. 
    ''')

    st.header('Coût chauffage par DPE - Moyenne en €')
    st.image('ressources/img/cout_chauffage_par_dpe_tharse.png')
    st.markdown('''
    On remarque que le coût moyen du chauffage n’évolue pas de manière linéaire selon la classe DPE. 
    Par exemple, les logements en catégorie A ont un coût très faible, autour de 25 €, 
    mais ceux en catégorie B ont un coût plus élevé, proche de 60 €, presque équivalent à ceux en catégorie G. 
    La non-linéarité peut s’expliquer par la diversité des usages, des tailles de logement, des systèmes de chauffage, et par les limites du DPE lui-même.
    ''')
    st.markdown('''Il est aussi possible que les écarts observés soient liés à une répartition déséquilibrée dans notre jeu de données : certaines classes DPE, comme A ou G, sont très peu représentées, ce qui peut fausser les moyennes et rendre les comparaisons moins fiables.''')

    st.header('Coût chauffage par Logement - Moyenne € — POSSIBLEMENT UNE ERREUR À VÉRIFIER (outlier sur les immeubles)')
    st.image('ressources/img/cout_chauffage_par_logement_tharse.png')
    st.header('Répartition de la consommation réelle vs estimée')
    st.image('/Users/tharse/projet-dpe/CHALLENGE-DPE-OPENDATA-UNIV/notebooks/comparaison_directe.png')
    st.header('Répartition de la consommation réelle vs estimée par DPE')
    st.image('//Users/tharse/projet-dpe/CHALLENGE-DPE-OPENDATA-UNIV/notebooks/comparaison_par_dpe.png')
    st.header('Distribution de la consommation / kWh/m²/an')
    st.header('Distribution de la consommation par DPE')
    st.header('Distribution de la consommation par type de logement')
    st.header('Écart type entre la consommation mesurée et la consommation prédite par le DPE, pour différents types de logements')
    st.image('notebooks/distribution_ecarts.png')
