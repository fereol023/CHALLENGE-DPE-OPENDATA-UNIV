from contents import *
import pickle
import gzip

def main():
    with gzip.open('ressources/models_fitted/.pickle', 'rb') as fichier:
        mdlrf = pickle.load(fichier)
    
    st.header('Preprocessing et testes d\'hypothèses')
    st.header('Modélisation')
    st.subheader('Modèle de régression linéaire')
    st.subheader('Modèle de régression par forêt aléatoire')
    st.subheader('Modèle de régression par arbre de décision')