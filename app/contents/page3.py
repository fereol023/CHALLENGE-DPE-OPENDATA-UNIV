from contents import *
import pickle
import gzip

opti_alphas_regressions_df = load_pickled_data_cache('ressources/models_fitted/regressions_lineaires/opti_alphas.pkl')
opti_alphas_regressions_df["r2"] = 100 * round(opti_alphas_regressions_df["r2"], 3)
opti_alphas_regressions_df["mape"] = round(100 * opti_alphas_regressions_df["mape"], 3)
opti_alphas_regressions_df["rmse"] = round(opti_alphas_regressions_df["rmse"], 3)
opti_alphas_regressions_df.rename(columns={
    'r2': 'R² (%)',
    'mse': 'MSE',
    'mape': 'MAPE (%)',
    'mae': 'MAE',
    'rmse': 'RMSE'
}, inplace=True)

# model : version : path
# load from yaml maybe or json
models = {
    'Régressions linéaires': {
        'ridge': {
            'path': 'ressources/models_fitted/ridge_top_10_features.png',
            'description': """
                pipeline de regression avec standard scaler sur les variables\
                quantitatives et ridge regression. 410 features obtenues dans le cas du onehot encoding.\
                            
                La regression ridge est une regression linéaire avec une penalisation L2.\
                La pénalisation L2 est une technique de regularisation qui ajoute une pénalité\
                sur la somme des carrés des coefficients de regression.

                La pénalité est le carré de la somme des coefficients de régression (poids).\
                Effet sur les poids : la régression ridge réduit les poids sans les annuler.\
                
                Utilisation : la régression ridge est utilisée lorsque toutes les features sont importantes.\
                La régression ridge ne fait pas de sélection explicite des features, mais elle peut réduire\
                l'importance des features moins pertinentes en réduisant leurs poids.\

                Cette penalisation permet de réduire le sur-apprentissage et d\'améliorer la\
                généralisation du modèle.\
                
                Le modèle a été évalué en utilisant la validation croisée et les résultats sont présentés ci-contre.
                """
        },
        'lasso': {
            'path': 'ressources/models_fitted/lasso_top_10_features.png',
            'description': """
                - pipeline de regression avec standard scaler sur les variables\
                quantitatives et ridge regression. 410 features obtenues dans le cas du onehot encoding.\

                - La regresssion Lasso est une regression linéaire avec une penalisation L1.\
                - La pénalisation L1 est une technique de regularisation qui ajoute une pénalité\
                sur la somme des carrés des coefficients de regression.\
                
                - La pénalité (norme L1) est la valeur absolue de la somme des coefficients de régression (poids).\
                - Effet sur les poids : la régression Lasso peut annuler certains poids, ce qui permet de\
                sélectionner certaines features et d'éliminer les autres.\
                
                - Cette penalisation permet de réduire le sur-apprentissage et d\'améliorer la\
                généralisation du modèle.\
                
                - Le modèle a été évalué en utilisant la validation croisée et les résultats sont présentés ci-contre.
                """
        },
        'simplereg': {
            'path': 'ressources/models_fitted/simple_top_10_features.png',
            'description': """
                pipeline de regression avec standard scaler sur les variables\
                quantitatives et ridge regression. 410 features obtenues dans le cas du onehot encoding.\
                La regression simple est une regression linéaire sans penalisation.\
                La régression simple est utilisée lorsque toutes les features sont considérées comme importantes.
                """
        },
    },
    'Régression par arbre de décision': {
        'dt_version_1': {
            'path': 'ressources/models_fitted/decision_tree_model_v1_carla.pkl',
            'description': 'description',
        }
    },
    'Régression par arbre par forêt aléatoire': {
        'rf_version_1': {
            'path': 'ressources/models_fitted/random_forest_model_v1_carla.pkl.zip',
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
        # list(opti_alphas_regressions_df.version_global.unique())
        list(models[model_selected].keys())
    )

    model_description = models[model_selected][model_selected_version]['description']
    
    st.header('Preprocessing et tests d\'hypothèses')
    st.header('Modélisation')
    st.markdown(f"""
    La modélisation de la consommation électrique a été réalisée à l'aide de plusieurs modèles
    statistiques et d'apprentissage automatique.
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
        st.markdown(
            """
            La recherche d'hyperparamètres a été effectuée pour optimiser les performances des modèles de régression linéaire. 
            Les résultats de cette recherche sont présentés ci-contre.
            """
        )        

        st.dataframe(
            opti_alphas_regressions_df.query(
                f"version_global == '{str(model_selected_version)}'"
            ).drop(columns=['model', 'name', 'version_global'])
        )
        st.markdown("""
            *Encoding (Etiquette DPE) : dans le cas du onehot encoding, on obtient des sous variables (0/1)
            pour chaque catégorie de la variable catégorielle alors que dans le cas du label encoding,
            on obtient une seule variable avec des valeurs numériques (0, 1, 2, ...).*
            
            *Ceci a permi de comparer la performance des modèles avec et sans le onehot encoding sur l'étiquette DPE.
            Comme montre le tableau des hyperparamètres, les modèle avec le onehot encoding ont donné de meilleurs résultats.*
            """)
        
        st.subheader(f'Interprétation des résultats - explicabilité modèle :orange[{model_selected_version}]')
        st.markdown(f"""
            Description du modèle choisi : :orange[{model_description}]
            """)

        if 'ridge' in model_selected_version:
            img = load_image('ressources/models_fitted/regressions_lineaires/ridge_top_10_features.png')
            if img: st.image(img, caption='Régression Ridge (L2) - coefficients')
        elif 'lasso' in model_selected_version:
            img = load_image('ressources/models_fitted/regressions_lineaires/lasso_top_10_features.png')
            if img: st.image(img, caption='Régression Lasso (L1) - coefficients')
        elif 'simplereg' in model_selected_version:
            img = load_image('ressources/models_fitted/regressions_lineaires/simple_top_10_features.png')
            if img: st.image(img, caption='Régression multiple - coefficients')
        else: 
            st.warning("Aucune recherche d'hyperparam. disponible pour la version sélectionnée.")
        
    elif model_selected == 'Régression par arbre de décision':
        st.subheader('Choix : Modèle de régression par arbre de décision')
        obj_model_dt = joblib.load(models[model_selected][model_selected_version]['path'])
        st.write(obj_model_dt)
        st.write(obj_model_dt.feature_names_in_)
        st.markdown(
            """
            La régression par arbre de décision est une méthode d'apprentissage supervisé 
            qui utilise un arbre de décision pour prédire une variable continue. 
            
            Dans ce cas, nous avons utilisé un arbre de décision pour modéliser la consommation électrique 
            en fonction des caractéristiques des logements. 
            
            Le modèle a été évalué sur les données de consommation électrique, et les résultats sont présentés ci-contre.
            """
        )
        img = load_image('ressources/models_fitted/arbre_decision_fereol.png')
        if img: st.image(img, caption='Régression par arbre de décision')
        
    elif model_selected == 'Régression par arbre par forêt aléatoire':
        st.subheader('Choix : Modèle de régression par forêt aléatoire')
        obj_model_rf = load_pickle_zipped(models[model_selected][model_selected_version]['path'])
        st.write(obj_model_rf)
        st.write(obj_model_rf.feature_names_in_)
        st.markdown(
            """
            La régression par forêt aléatoire est une méthode d'apprentissage supervisé 
            qui utilise un ensemble d'arbres de décision pour prédire une variable continue. 
            
            Dans ce cas, nous avons utilisé une forêt aléatoire pour modéliser la consommation électrique 
            en fonction des caractéristiques des logements. 
            
            Le modèle a été évalué sur les données de consommation électrique, et les résultats sont présentés ci-contre.
            """
        )
        img = load_image('ressources/models_fitted/mdlrf.png')
        if img: st.image(img, caption='Régression par forêt aléatoire')
    else:
        st.error("Modèle non reconnu. Veuillez sélectionner un modèle valide.")
        return