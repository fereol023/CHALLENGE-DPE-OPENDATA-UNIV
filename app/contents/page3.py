from contents import *
import pickle
import gzip

# model : version : path
# load from yaml maybe or json
models = {
    'Régressions linéaires': {
        'ridge_version_1': {
            'path': 'ressources/models/mdlrf.pkl',
            'description': 'pipeline de regression avec standard scaler sur les variables quantitatives et ridge regression'
        },
        'ridge_version_2': {
            'path': 'ressources/models/mdlrf.pkl',
            'description': 'description'
        },
        'lasso_version_1': {
            'path': 'ressources/models/mdlrf.pkl',
            'description': 'description'
        },
        'linear_simple_version_1': {
            'path': 'ressources/models/mdlrf.pkl',
            'description': 'description'
        },
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

    st.header("Modélisation de la consommation électrique (kWh/m²/an)")
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

    model_description = models[model_selected][model_selected_version]['description']
    obj_model = load_model(models[model_selected][model_selected_version]['path'])
    
    st.header('Preprocessing et testes d\'hypothèses')
    
    st.header('Preprocessing et tests d\'hypothèses')
    st.header('Modélisation')
    st.markdown(f"""
    La modélisation de la consommation électrique a été réalisée à l'aide de plusieurs modèles
    statistiques et d'apprentissage automatique.
    
    Description du modèle choisi :
    :orange[{model_description}]
                """
                )
    if model_selected == 'Régressions linéaires':
        st.subheader('Choix : Modèles de regression linéaire')
        c11, c12 = st.columns(2)
        c11.markdown(
            """
            Les régressions linéaires sont des modèles statistiques utilisés pour prédire 
            une variable cible continue en fonction d'une ou plusieurs variables explicatives. 
              
            Dans ce contexte, nous avons testé plusieurs variantes de régressions linéaires, 
            notamment Ridge, Lasso, et une régression linéaire simple. 
            
            Chaque modèle a été évalué en fonction de ses performances sur 
            les données de consommation électrique, et les résultats sont comparés dans le graphique ci-contre.
            """
        )
        c12.image(
            load_image('ressources/models_fitted/regressions_lineaires/regressions_comparaison_fereol.png'), 
            caption='Comparaison des régressions linéaires simple, ridge (L2) et lasso (L1)',
            )
        
        st.subheader(f"Recherche d'hyperparamètres : :orange[{model_selected_version}]")
        if 'ridge' in model_selected_version:
            st.image(
                load_image('ressources/models_fitted/regressions_lineaires/opti_alphas_.png'), 
                caption='Régression Ridge (L2)',
                )
        elif 'lasso' in model_selected_version:
            st.image(
                load_image('ressources/models_fitted/regressions_lineaires/lasso_opti_alphas_fereol.png'), 
                caption='Régression Lasso (L1)',
                )
        else: 
            st.warning("Aucune recherche d'hyperparam. disponible pour la version sélectionnée.")
        
        
    elif model_selected == 'Régression par arbre de décision':
        st.subheader('Choix : Modèle de régression par arbre de décision')
        c21, c22 = st.columns(2)
        c21.markdown(
            """
            La régression par arbre de décision est une méthode d'apprentissage supervisé 
            qui utilise un arbre de décision pour prédire une variable continue. 
            
            Dans ce cas, nous avons utilisé un arbre de décision pour modéliser la consommation électrique 
            en fonction des caractéristiques des logements. 
            
            Le modèle a été évalué sur les données de consommation électrique, et les résultats sont présentés ci-contre.
            """
        )
        c22.image(
            load_image('ressources/models_fitted/arbre_decision_fereol.png'), 
            caption='Régression par arbre de décision',
            )
        
    elif model_selected == 'Régression par arbre par forêt aléatoire':
        st.subheader('Choix : Modèle de régression par forêt aléatoire')
        c31, c32 = st.columns(2)
        c31.markdown(
            """
            La régression par forêt aléatoire est une méthode d'apprentissage supervisé 
            qui utilise un ensemble d'arbres de décision pour prédire une variable continue. 
            
            Dans ce cas, nous avons utilisé une forêt aléatoire pour modéliser la consommation électrique 
            en fonction des caractéristiques des logements. 
            
            Le modèle a été évalué sur les données de consommation électrique, et les résultats sont présentés ci-contre.
            """
        )
        c32.image(
            load_image('ressources/models_fitted/forêt_aléatoire_fereol.png'), 
            caption='Régression par forêt aléatoire',
            )
    else:
        st.error("Modèle non reconnu. Veuillez sélectionner un modèle valide.")
        return