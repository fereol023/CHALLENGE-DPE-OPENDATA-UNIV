from contents import *

def get_model_features_config():
    """
    Get the features used in the model.
    """
    from ressources.models_fitted.random_forests.model_features import df_summary_with_mapping
    return df_summary_with_mapping

model_metadata = {
    "path": "ressources/models_fitted/random_forests/random_forest_model_v1_restreint.pkl.zip",
    "type": "Random Forest",
    "version": "rf_version_1",
    "train_scope": None,
    "description": "Modèle de Random Forest pour la prédiction de la consommation électrique",
    "model_state": None,
    "features": get_model_features_config()
}

def compute_remaining_dpe_labels(dpe_value):
    """
    Pour obtenir la liste des dpe restants. 
    Prédire les économies réalisées en changeant de classe DPE
    """
    mapping = {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6, 'G': 7}
    if dpe_value not in mapping: # alors c'est un NA qui a été envoyé
        return list(mapping.keys())
    tmp = mapping.copy()
    tmp.pop(dpe_value)
    return list(tmp.keys())


def main(selected_ville, selected_annee, model_metadata=model_metadata):
    st.title("Démo de la modélisation")
    st.header("Modélisation de la consommation électrique (kwh/m2/an)")

    loaded_model = load_pickle_zipped(model_metadata["path"])
    model_state = ":green[loaded]" if loaded_model else ":red[error/not loaded]"

    st.subheader("Metadata modèle")
    st.markdown(f"""
**Type :** *{model_metadata['type']}*\n
**Path :** *{model_metadata['path']}*\n
**Version :** *{model_metadata['version']}*\n
**Train scope :** *{selected_ville}* *{selected_annee}*\n
**Description :** *{model_metadata['description']}*\n
**Model state :** *{model_state}*       
                """)

    st.markdown("--------------------")
    if loaded_model:
        st.subheader("Prédictions")
        model_features_config: dict = get_model_features_config()
        # st.write(model_features_config)
        # formulaire de saisie sur 3 colonnes à partir de model_fetures
        FLOAT_COLS, CATEG_COLS = [], []
        for feature, config in model_features_config.items():
            if 'float' in config.get('dtype'):
                FLOAT_COLS.append(feature)
            elif 'int' in config.get('dtype'):
                CATEG_COLS.append(feature)
            else:
                pass

        input_values = {}
        col11, col12 = st.columns(2)
        _milieu = len(FLOAT_COLS)//2
        for feature in FLOAT_COLS[:_milieu]:
            # default is min+max//2
            config = model_features_config.get(feature)
            _default = (config.get('min', 0) + config.get('max', 100)) // 2
            input_values[feature] = col11.slider(
                f"{feature} {config.get('unit', '')}",
                min_value=float(config.get('min', 0)),
                max_value=float(config.get('max', 100)),
                value=_default
            )
        for feature in FLOAT_COLS[_milieu:]:
            # default is min+max//2
            config = model_features_config.get(feature)
            _default = (config.get('min', 0) + config.get('max', 100)) // 2
            input_values[feature] = col12.slider(
                f"{feature} {config.get('unit', '')}",
                min_value=float(config.get('min', 0)),
                max_value=float(config.get('max', 100)),
                value=_default
            )
            
        col21, col22 = st.columns(2)
        _milieu = len(CATEG_COLS)//2
        for feature in CATEG_COLS[:_milieu]:
            config = model_features_config.get(feature)
            v = col21.selectbox(f"{feature}", options=config['mapping'], index=0)
            input_values[feature] = config.get('mapping').get(v)
        for feature in CATEG_COLS[_milieu:]:
            config = model_features_config.get(feature)
            v = col22.selectbox(f"{feature}", options=config['mapping'], index=0)
            input_values[feature] = config.get('mapping').get(v)

        input_dpe_value = input_values.get('etiquette_dpe_ademe')
        inputs_model = []
        for dpe in range(1, 8):
            input_values['etiquette_dpe_ademe'] = dpe
            inputs_model.append(input_values.copy())
        
        input_model_df = pd.DataFrame(inputs_model)
        input_model_df.sort_index(axis=1, inplace=True)
        # st.dataframe(input_df)

        # bouton de prédiction
        if st.button("Prédire"):

            # 7 predictions - DPE A à G
            # prédire avec les mêmes inputs values les conso kwh/an/m2 et conso kwh/an
            # pour toutes les variantes du DPE 
            prediction = loaded_model.predict(input_model_df)

            rev_dpe_enc = {1: "A", 2: "B", 3: "C", 4: "D", 5: "E", 6: "F", 7: "G"}
            # ajouter les predictions to the DataFrame
            input_model_df['Conso kwh/m2/an'] = prediction
            input_model_df['Conso kwh/an'] = input_model_df['Conso kwh/m2/an'] * input_model_df['surface_habitable_logement_ademe']
            input_model_df['Etiquette DPE'] = input_model_df['etiquette_dpe_ademe'].apply(lambda r: rev_dpe_enc.get(r))
            res_cols = [
                "Etiquette DPE",
                "Conso kwh/m2/an",
                "Conso kwh/an",
                "surface_habitable_logement_ademe"
            ]
            res_df = input_model_df[res_cols].rename(
                columns={
                    "surface_habitable_logement_ademe": "Surface du logement",
                    })
            
            st.dataframe(res_df)
            st.success(f"""
                **Résultats :**\n
                "➡️ Etiquette DPE : {input_dpe_value}\n
                "✅ Conso kwh/an": *{round(prediction[0]*input_values.get("surface_habitable_logement_ademe"), 3)}*\n
                "✅ Conso kwh/m2/an": *{round(prediction[0], 3)}*,\n
                """)
