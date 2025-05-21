example = {
    "conso_kwh_m2":191.711,
    "apports_solaires_saison_chauffe_ademe":150.0,
    "besoin_chauffage_ademe":342.7,
    "conso_5_usages_e_finale_energie_ndeg2_ademe":437.8,
    "conso_5_usages_m2_e_finale_ademe":78.0,
    "conso_5_usages_par_m2_e_primaire_ademe":180.0,
    "conso_auxiliaires_e_primaire_ademe":339.8,
    "conso_e_finale_depensier_installation_ecs_ademe":1555.800,
    "conso_ecs_depensier_e_primaire_ademe":2760.89,
    "conso_ecs_e_finale_energie_ndeg2_ademe":0.0,
    "consommation_annuelle_totale_de_l_adresse_mwh_enedis_with_ban":85.1,
    "cout_auxiliaires_ademe":51.2,
    "deperditions_baies_vitrees_ademe":3.09,
    "deperditions_planchers_hauts_ademe":0.0,
    "emission_ges_5_usages_energie_ndeg2_ademe":27.7,
    "emission_ges_5_usages_par_m2_ademe":5.0,
    "etiquette_dpe_ademe":3,
    "surface_habitable_logement_ademe":22.2,
    "type_energie_generateur_ndeg1_installation_ndeg1_ademe":2,
    "type_energie_ndeg1_ademe":2,
    "type_energie_ndeg2_ademe":12,
    "type_installation_ecs_general_ademe":1,
    "ubat_w_m2_k_ademe":1.059,
    "usage_generateur_ecs_ndeg1_ademe":2,
    "volume_stockage_generateur_ecs_ndeg1_ademe":65.0
    }

df_summary_with_mapping = {
    'apports_solaires_saison_chauffe_ademe': {
        'min': 0.0,
        'max': 1_500,
        'def': 150,
        'dtype': 'float32'
    },
    'besoin_chauffage_ademe': {
        'min': 0.4,
        'max': 8_000,
        'def': 342,
        'dtype': 'float32'
    },
    'conso_5_usages_e_finale_energie_ndeg2_ademe': {
        'min': 0.7,
        'max': 1_000,
        'def': 437, 
        'dtype': 'float32'
    },
    'conso_5_usages_m2_e_finale_ademe': {
        'min': 0.5,
        'max': 7_690.9,
        'def': 78,
        'dtype': 'float32'
    },
    'conso_5_usages_par_m2_e_primaire_ademe': {
        'min': 13.9,
        'max': 8_083.9,
        'def': 180,
        'dtype': 'float32'
    },
    'conso_auxiliaires_e_primaire_ademe': {
        'min': 0.0,
        'max': 8_517.8,
        'def': 339,
        'dtype': 'float32'
    },
    'conso_e_finale_depensier_installation_ecs_ademe': {
        'min': 0.0,
        'max': 3_039.2,
        'def': 1_500.8,
        'dtype': 'float32'
    },
    'conso_ecs_depensier_e_primaire_ademe': {
        'min': 0.0,
        'max': 3_039.2,
        'def': 2760,
        'dtype': 'float32'
    },
    'conso_ecs_e_finale_energie_ndeg2_ademe': {
        'min': 0.0,
        'max': 605.4,
        'def': 0,
        'dtype': 'float32'
    },
    'consommation_annuelle_totale_de_l_adresse_mwh_enedis_with_ban': {
        'min': 2.166,
        'max': 1_218.589,
        'def': 85.1,
        'dtype': 'float32'
    },
    'cout_auxiliaires_ademe': {
        'min': 0.0,
        'max': 287.0,
        'def': 51.2,
        'dtype': 'float32'
    },
    'deperditions_baies_vitrees_ademe': {
        'min': 0.0,
        'max': 58.0,
        'def': 3.09,
        'dtype': 'float32'
    },
    'deperditions_planchers_hauts_ademe': {
        'min': 0.0,
        'max': 130.001,
        'def': 0,
        'dtype': 'float32'
    },
    'emission_ges_5_usages_energie_ndeg2_ademe': {
        'min': 0.0,
        'max': 166.49,
        'def': 27.7,
        'dtype': 'float32'
        },
    'emission_ges_5_usages_par_m2_ademe': {
        'min': 0.0,
        'max': 17.00,
        'def': 5.0,
        'dtype': 'float32'
        },
    'etiquette_dpe_ademe': {
        'min': 1,
        'max': 7,
        'dtype': 'int32',
        'mapping': {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6, 'G': 7, 'NA': -1}
        },
    'surface_habitable_logement_ademe': {
        'min': 2,
        'max': 1_500.0,
        'def': 22.2,
        'dtype': 'float32'
        },
    'type_energie_generateur_ndeg1_installation_ndeg1_ademe': {
        'min': 1,
        'max': 12,
        'dtype': 'int32',
        'mapping': {
            'Gaz naturel': 1,
            'Électricité': 2,
            'Fioul domestique': 3,
            'Réseau de Chauffage urbain': 4,
            'GPL': 5,
            'Bois – Bûches': 6,
            'Propane': 7,
            'Bois – Plaquettes forestières': 8,
            'Butane': 9,
            'Charbon': 10,
            'Bois – Granulés (pellets) ou briquettes': 11,
            'NA': 12,
            }
        },
    'type_energie_ndeg1_ademe': {
        'min': 1,
        'max': 11,
        'dtype': 'int32',
        'mapping': {
            'Gaz naturel': 1,
            'Électricité': 2,
            'Fioul domestique': 3,
            'Réseau de Chauffage urbain': 4,
            'GPL': 5,
            'Bois – Bûches': 6,
            'Butane': 7,
            'Charbon': 8,
            'Bois – Granulés (pellets) ou briquettes': 9,
            'Propane': 10,
            'Réseau de Froid Urbain': 11,
            'NA': -2
            }
        },
    'type_energie_ndeg2_ademe': {
        'min': 1,
        'max': 12,
        'dtype': 'int32',
        'mapping': {
            'Électricité': 1,      
            'Gaz naturel': 2,                                                    
            'Réseau de Chauffage urbain': 3,                                      
            'Bois – Bûches': 4,                                                  
            'Fioul domestique': 5,                                                
            'GPL': 6,                                                             
            "Électricité d'origine renouvelable utilisée dans le bâtiment": 7,    
            'Bois – Plaquettes d’industrie': 8,                                   
            'Bois – Granulés (pellets) ou briquettes': 9,                          
            'Bois – Plaquettes forestières': 10,                                  
            'Réseau de Froid Urbain': 11,                                         
            'NA': 12                                                            
            }
        },
    'type_installation_ecs_general_ademe': {
        'min': 1,
        'max': 4,
        'dtype': 'int32',
        'mapping': {
            'individuel': 1,
            'collectif': 2,              
            'mixte (collectif-individuel)': 3,
            'NaN': 4
            },
        },
    'ubat_w_m2_k_ademe': {
        'min': 0.0,
        'max': 25.43,
        'def': 1.059,
        'dtype': 'float32'
        },
    'usage_generateur_ecs_ndeg1_ademe': {
        'min': 1,
        'max': 4,
        'dtype': 'int32',
        'mapping': {
            'chauffage + ecs': 1,
            'ecs': 2, 
            'chauffage': 3,
            'NA': 4
            }
        },
    'volume_stockage_generateur_ecs_ndeg1_ademe': {
        'min': 0.0,
        'max': 65.0,
        'def': 65,
        'dtype': 'float32'
        }
}