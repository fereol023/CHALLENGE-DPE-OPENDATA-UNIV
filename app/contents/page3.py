from contents import *
import pickle
import gzip

# model : version : path
# load from yaml maybe or json config
models = {
    'Régression ridge': {
        'ridge_version_1': {
            'path': 'ressources/models/mdlrf.pkl',
            'description': 'description'
        }
    },
    'Régression lasso': {
        'lasso_version_1': {
            'path': 'ressources/models/mdlrf.pkl',
            'description': 'description'
        }
    },
    'Régression par arbre de décision': {
        'dt_version_1': {
            'path': 'ressources/models/mdlrf.pkl',
            'description': 'description'
        }
    },
    'Régression par arbre par forêt aléatoire': {
        'rf_version_1': {
            'path': 'ressources/models/mdlrf.pkl',
            'description': 'description'
        }
    },
}

def load_model(file_path):
    pass

def main(selected_ville, selected_annee):

    st.subheader("Modélisation de la consommation électrique (kWh/m²/an)")
    st.write(f"Ville sélectionnée : {selected_ville}")
    st.write(f"Année sélectionnée : {selected_annee}")

    st.sidebar.markdown("-------------------")
    st.sidebar.header('Choix modèle')
    model_selected = st.sidebar.selectbox(
        'Sélectionner le modèle',
        list(models.keys())
    )
    model_selected_version = st.sidebar.radio(
        'Sélectionner la version du modèle',
        list(models[model_selected].keys())
    )

    fichier = models[model_selected][model_selected_version]['path']
    mdlrf = load_model(fichier)
    
    st.header('Preprocessing et testes d\'hypothèses')
    st.header('Modélisation')
