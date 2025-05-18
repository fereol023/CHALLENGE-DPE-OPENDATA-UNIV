df_summary_with_mapping = {
    'apports_solaires_saison_chauffe_ademe': {
        'min': 0.0,
        'max': 1_555_918_700,
        'dtype': 'float32'
    },
    'besoin_chauffage_ademe': {
        'min': 0.4,
        'max': 8_436_272,
        'dtype': 'float32'
    },
    'conso_5_usages_e_finale_energie_ndeg2_ademe': {
        'min': 0.7,
        'max': 877_816,
        'dtype': 'float32'
    },
    'conso_5_usages_m2_e_finale_ademe': {
        'min': 0.5,
        'max': 7_690.9,
        'dtype': 'float32'
    },
    'conso_5_usages_par_m2_e_primaire_ademe': {
        'min': 13.9,
        'max': 8_083.9,
        'dtype': 'float32'
    },
    'conso_auxiliaires_e_primaire_ademe': {
        'min': 0.0,
        'max': 851_761.8,
        'dtype': 'float32'
    },
    'conso_e_finale_depensier_installation_ecs_ademe': {
        'min': 0.0,
        'max': 3_039_539.2,
        'dtype': 'float32'
    },
    'conso_ecs_depensier_e_primaire_ademe': {
        'min': 0.0,
        'max': 3_039_539.2,
        'dtype': 'float32'
    },
    'conso_ecs_e_finale_energie_ndeg2_ademe': {
        'min': 0.0,
        'max': 605_106.4,
        'dtype': 'float32'
    },
    'consommation_annuelle_totale_de_l_adresse_mwh_enedis_with_ban': {
        'min': 2.166,
        'max': 1_218.589,
        'dtype': 'float32'
    },
    'cout_auxiliaires_ademe': {
        'min': 0.0,
        'max': 287_001.0,
        'dtype': 'float32'
    },
    'deperditions_baies_vitrees_ademe': {
        'min': 0.0,
        'max': 58_701.0,
        'dtype': 'float32'
    },
    'deperditions_planchers_hauts_ademe': {
        'min': 0.0,
        'max': 130_011.9,
        'dtype': 'float32'
    },
    'emission_ges_5_usages_energie_ndeg2_ademe': {
        'min': 0.0,
        'max': 166_466.9,
        'dtype': 'float32'
        },
    'emission_ges_5_usages_par_m2_ademe': {
        'min': 0.0,
        'max': 1700.0,
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
        'max': 1000.0,
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
        'max': 65000.0,
        'dtype': 'float32'
        }
}