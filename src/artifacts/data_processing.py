import warnings, os 
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')

from src.artifacts import MLFlowExp
from utils.fonctions import get_entropy, compute_entropies, normalize_df_colnames, get_today_date

import sys, re, datetime, numpy as np
from utils.fonctions import normalize_colnames_list, normalize_df_colnames


class TestExp(MLFlowExp):
    """Classe test pour le fonctionnement de mlflow"""
    def __init__(self, experiment_name="test"):
        self.experiment_name = experiment_name
        super().__init__(self.experiment_name)
    
    def run(self):
        print(f'Running : {self.experiment_name}')


class DataPreprocessor(MLFlowExp):
    def __init__(self, df, experiment_name='0_dataset_cleaning'):
        self.experiment_name = experiment_name
        super().__init__(self.experiment_name)
        self.nettoyage = NettoyageWithMLflow(df, self.experiment_name, self.mlflow)

    def run(self):
        try:
            self.nettoyage.run()
            return self
        except Exception as e:
            print(f"Exception occured while cleaning data : {e}")
    
    def save(self, to_path=None):
        if not os.path.exists(to_path):
            os.makedirs(os.path.dirname(to_path))
        try:
            self.nettoyage.df.to_parquet(to_path, engine='pyarrow', compression='gzip')
            self.mlflow.log_artifact(to_path, artifact_path=f"data_pipelines/cleaning_{get_today_date()}")
        except Exception as e:
            print(f"Exception occured while saving data processed : {e}")


class Nettoyage:
    """Classe principale qui gère le nettoyage d'un df."""

    to_drop = ["street_enedis_with_ban", "name_enedis_with_ban", "id_BAN_enedis_with_ban", "housenumber_enedis_with_ban", "label_enedis_with_ban", "full_adress_enedis_with_ban",
            "adresse_enedis_with_ban","Libellé de voie_enedis_with_ban", "Type de voie_enedis_with_ban", "Nom IRIS_enedis_with_ban", "N°_DPE_immeuble_associé_ademe", "Nom__rue_(BAN)_ademe",
            "Identifiant__BAN_ademe","Complément_d'adresse_bâtiment_ademe", "Adresse_brute_ademe", "N°DPE_ademe", "N°_voie_(BAN)_ademe", "Complément_d'adresse_logement_ademe",
            "Adresse_(BAN)_ademe","_geopoint_ademe","Statut_géocodage_ademe", "Description_installation_ECS_ademe", "Description_générateur_ECS_n°1_ademe",
            "Description_générateur_chauffage_n°1_installation_n°1_ademe", "Description_installation_chauffage_n°1_ademe", 
            "score_enedis_with_ban", "x_enedis_with_ban", "y_enedis_with_ban","importance_enedis_with_ban", "unnamed_0_enedis_with_ban"]

    def __init__(self, df, cols_to_delete_mano=[], inplace=False):
        self.df = df if inplace else df.copy()
        self.df = normalize_df_colnames(self.df)
        self.cols_to_delete_mano = list(set(cols_to_delete_mano)) + Nettoyage.to_drop
        self.cols_to_delete_mano = normalize_colnames_list(self.cols_to_delete_mano)
        self.variables_deleted = {"mano": self.cols_to_delete_mano}
        self.variables_typed = {}
        self.cols_not_to_deleted = normalize_colnames_list(["Consommation annuelle moyenne par logement de l'adresse (MWh)_enedis_with_ban"])

    def delete_cols_to_delete_mano(self):
        print("-> Delete cols mano..")
        missings = set(self.cols_to_delete_mano) - set(self.df.columns)
        assert missings==set(), f"Il manque des colonnes parmi celles à supprimer à la mano : {missings}"
        for c in self.cols_not_to_deleted:
            if c in self.cols_to_delete_mano:
                self.cols_to_delete_mano.remove(c)
        self.df = self.df.drop(self.cols_to_delete_mano, axis=1)
        return self

    def get_entropy(self, pk, L):
        op= - (pk * np.log(pk) / np.log(L)).sum()
        return op    

    def get_entropy_by_colname(self, colname):
        tauxnan = round(self.df[colname].isna().sum()/len(self.df),2)
        pk = self.df[colname].value_counts(normalize=True, dropna = False).values
        return round(self.get_entropy(pk, len(self.df)), 2)
    
    def get_corr_with_target_by_colname(self, colname, col_target='consommation_annuelle_moyenne_par_logement_de_l_adresse_mwh_enedis_with_ban'):
        assert col_target in self.df.columns, f"Erreur la variable target {col_target} n'est pas dans le dataframe à nettoyer. préciser un autre nom ou corriger orthographe."
        return self.df[[colname, col_target]].corr().iloc[0, 1]

    def delete_colnan(self, taux_seuil):
        print("-> Delete NaN cols..")
        print("Le dataframe contient initialement {} colonnes et {} lignes.".format(len(self.df.columns), len(self.df)))
        print("Start processing : columns identification..")
        
        nanames = []
        entro = []
        extra = ["Code_INSEE_(BAN)_ademe",
                 "Code IRIS_enedis_with_ban",
                 "Code_postal_(brut)_ademe",
                 "Code_postal_(BAN)_ademe",
                 "Numéro de voie_enedis_with_ban",
                 "Nombre de logements_enedis_with_ban",
                 "Tri des adresses_enedis_with_ban",
                 "postcode_enedis_with_ban",
                 "citycode_enedis_with_ban"]
        extra = normalize_colnames_list(extra)
        extra = [c for c in extra if c in self.df.columns]

        for c in self.df.columns:
            tauxnan = round(self.df[c].isna().sum()/len(self.df),2)
            pk = self.df[c].value_counts(normalize=True, dropna = False).values
            entropy = round(self.get_entropy(pk, len(self.df)),2)
            if tauxnan >= taux_seuil:
                nanames.append(c)
            if c[0]=="_" and "geo" not in c: 
                extra.append(c)
            if entropy==1 or entropy==0: # suppr les colonnes homogènes et les clés primaires
                entro.append(c)

        print("Il y a {} colonnes vides à au moins, {}%, {} colonnes avec une entropie de 0 ou 1 et {} colonnes inutiles".format(len(nanames), taux_seuil*100, len(entro), len(extra)))
        print("Start processing : columns deleting ...")
        for c in self.cols_not_to_deleted:
            if c in nanames:
                nanames.remove(c)
            if c in entro:
                entro.remove(c)
            if c in extra:
                extra.remove(c)
        self.df = self.df.drop(nanames+entro+extra, axis=1).drop_duplicates()
        print("Il reste {} colonnes et {} lignes.".format(len(self.df.columns), len(self.df)))
        # update
        self.variables_deleted.update({"low_or_high_entropy": entro})
        self.variables_deleted.update({"extra_columns_not_interesting": extra})
        self.variables_deleted.update({"nan_columns": nanames})
        return self

    def auto_cast_object_columns(self):
        """Automatic casting for object columns.
        
        Technique :
        On teste le cast en numeric, si ca fail on teste le cast en datetime, si ca fail on laisse en str.
        """
        
        print("-> Start object columns auto casting..")
        cols_obj = self.df.select_dtypes(include='O').columns
        for c in cols_obj:
            # self.df[c] = self.df[c].str.replace(',', '.')
            # self.df[c] = self.df[c].fillna(-999999)
            try:
                self.df[c] = pd.to_numeric(self.df[c].str.replace(',','.'), errors='raise')
            except Exception as e:
                try:
                    self.df[c] = pd.to_datetime(self.df[c])
                except Exception as e:
                    self.df[c] = self.df[c].astype('string')
        print("End casting")
        return self
    

    def fillnan_float_dtypes(self):
        """
        Il est conseillé de faire un fillna par la médiane si on a une variable avec des outliers et
          de faire une imputation par la moyenne sinon.

        Technique : 
        on calcule les bornes de l'IQR et on vérifie si on des obs superieurs ou inferieures à bsup et binf.
        si oui, on fait une imputatin par la médiane, si non on fait une imputation par la moyenne. 
        """
        col_fill_median, col_fill_mean = [], []
        print("-> Fill pd.NA for float dtypes..")
        for col in self.df.select_dtypes(include ='float').columns:
            if self.df[col].isna().any():
                Q1 = self.df[col].quantile(0.25)
                Q3 = self.df[col].quantile(0.75)
                IQR = Q3 - Q1
                born_inf = (self.df[col]<(Q1-1.5*IQR)).value_counts()
                born_sup = (self.df[col]>(Q3+1.5*IQR)).value_counts()
                try:
                    born_inf[1]
                    self.df[col].fillna(self.df[col].median(), inplace=True)
                    col_fill_median.append(col)
                except Exception:
                    try:
                        born_sup[1]
                        self.df[col].fillna(self.df[col].median(), inplace=True)
                        col_fill_median.append(col)
                    except Exception:
                        self.df[col] = self.df[col].fillna(self.df[col].mean())
                        col_fill_mean.append(col)
        print("Done fillnan !")
        # update
        self.variables_typed.update({"fillna by mean": col_fill_mean})
        self.variables_typed.update({"fillna by median": col_fill_median})
        return self

    def auto_clean_correlation(self, seuil, select_with_entropy=False, select_with_target_correlation=False):
        """
        Clean and drop columns (auto) based on correlation.

        Technique :
        seuil = seuil au deà du quel on considère que 2 variables sont trop liées.
        Fais la matrice de correlation (Y vs X) -> si le coeff est elevé supprime Y (un peu arbitraire)
        (compare la valeur aboslue du coef au seuil)
        (correction - selection arbitraire)
        entre 2 variables fortement correlées, 
        - on garde celle qui est le plus correlé à la target
        - ou celle qui dont l'entropie est la plus elevée
        """
        col = [c for c in self.df.select_dtypes("float").columns]
        correlation_matrix = self.df[col].corr()

        threshold = seuil  # le seuil de colinéarité
        upper_triangle = correlation_matrix.where(np.triu(np.ones(correlation_matrix.shape), k=1).astype(bool))

        # trouver les colonnes ayant une corrélation supérieure au seuil
        to_drop = []
        for column in upper_triangle.columns:
            
            high_corr = upper_triangle[column][abs(upper_triangle[column]) > 0.9].index.tolist()
            for col in high_corr:
                if select_with_entropy:
                    if self.get_entropy_by_colname(column) > self.get_entropy_by_colname(col):
                        to_drop.append(col)
                    else:
                        to_drop.append(column)
                
                if select_with_target_correlation:
                    if self.get_corr_with_target_by_colname(column) > self.get_corr_with_target_by_colname(col):
                        to_drop.append(col)
                    else:
                        to_drop.append(column)
        
        to_drop = list(set(to_drop))
        # to_drop = [column for column in upper_triangle.columns if any(abs(upper_triangle[column]) > threshold)]
        for c in self.cols_not_to_deleted:
            if c in to_drop:
                to_drop.remove(c)
        print(f"Les colonnes suivantes ont une corrélation supérieure à {threshold} : {to_drop}")

        # supprimer les colonnes colinéaires
        self.df = self.df.drop(columns=to_drop)

        print(f"Les colonnes suivantes ont été supprimées : {to_drop}")
        print(f'Il reste {len(self.df.columns)} colonnes')

        self.variables_deleted.update({"high_correlation": to_drop})
        return self
    
    def extract_digit(self, x):
        return re.sub(r'\D', '', str(x))

    def compute_arrondissement(self):
        self.df["arrondissement"] = self.df["district_enedis_with_ban"].apply(self.extract_digit).astype('string')
        self.df = self.df.drop('district_enedis_with_ban', axis=1)
        return self
    
    def compute_target(self):
        target = normalize_colnames_list(["Consommation annuelle moyenne par logement de l'adresse (MWh)_enedis_with_ban"])[0]
        # new_target = target.replace('mwh', 'kwh')
        if (target in self.df.columns) and ('surface_habitable_logement_ademe' in self.df.columns) :
            self.df['conso_kwh_m2'] = self.df[target] *1000 / self.df['surface_habitable_logement_ademe']
            self.df = self.df.drop(columns=[target], axis=1)
        return self
    

    def run(self, compute_target=True, use_entropy_selection=False, use_target_correlation_selection=False):
        d = datetime.datetime.now()
        if self.cols_to_delete_mano:
            self.delete_cols_to_delete_mano()
        # run
        self.delete_colnan(taux_seuil=0.9)\
            .auto_cast_object_columns()\
            .fillnan_float_dtypes()\
            .auto_clean_correlation(seuil=0.9,select_with_entropy=use_entropy_selection, select_with_target_correlation=use_target_correlation_selection)\
            .compute_arrondissement()
        
        _date_cols = self.df.select_dtypes(include=['datetime', 'datetime64', 'datetime64[ns]']).columns
        print(f"Il y a {len(_date_cols)} colonnes de dates à supprimer..")
        self.df = self.df.drop(columns=_date_cols, axis=1)
        self.df = self.df.drop_duplicates().reset_index(drop=True)
        if compute_target:
            self.compute_target()
        print(f"Le nettoyage a duré : {datetime.datetime.now()-d}")
        print("="*100)


class NettoyageWithMLflow(Nettoyage):

    def __init__(self, df, experiment_name, _mlflow):
        self.df = normalize_df_colnames(df)
        self.cols_to_delete = set()
        self.df_entropies = None
        self.delete_colnan_step = False
        self.fillna_step = False
        self.cast_object_columns_step = False
        self.delete_based_on_entropies_step = False
        self.mlflow = _mlflow
        
    def delete_colnan(self, tauxseuil=0.9):
        nanames, entro, extra = [], [], []
        if not self.delete_colnan_step:
            print("Le dataframe contient initialement ", len(self.df.columns)," colonnes.")
            print("Start processing : columns identification..")
            for c in self.df.columns:
                tauxnan = round(self.df[c].isna().sum()/len(self.df),2)
                pk = self.df[c].value_counts(normalize=True, dropna = False).values
                entropy = round(get_entropy(pk, len(self.df)),2)
                if tauxnan >= tauxseuil:
                    nanames.append(c)
                if c.startswith('_') and ('geo' not in c.lower()):
                    extra.append(c)
                if entropy == 1 or entropy == 0:
                    entro.append(c)

            print("Il y a {} colonnes vides avec au moins {}% , {} colonnes avec une entropy de 1 ou 0 et {} colonnes inutiles.".format(len(nanames),tauxseuil*100, len(entro), len(extra)))
            self.cols_to_delete.update(nanames+extra+entro)
            if len(self.cols_to_delete) > 0:
                print("Start processing : columns deleting ...")
                self.df = self.df.drop(list(self.cols_to_delete), axis=1)
                print("Done")
                print(f"Il reste {len(self.df.columns)} colonnes.")
                
                self.mlflow.log_metric("nan_cols", len(nanames))
                self.mlflow.log_metric("entropy_cols", len(entro))
                self.mlflow.log_metric("extra_cols", len(extra))

            self.delete_colnan_step = True
        return self
        

    def cast_object_columns(self):
        if not self.cast_object_columns_step:
            cols_obj = self.df.select_dtypes(include='O').columns
            for c in cols_obj:
                self.df[c] = self.df[c].replace(',', '.')
                # self.df[c] = self.df[c].fillna(-999999)
                try:
                    self.df[c] = pd.to_numeric(self.df[c], errors='raise')
                except Exception as e:
                    try:
                        self.df[c] = pd.to_datetime(self.df[c])
                    except Exception as e:
                        self.df[c] = self.df[c].astype('string')
            print("Casting done...")
            self.cast_object_columns_step = True
        return self
    
    # deprecated delete
    def delete_based_on_entropies(self): # calculer sur un process à part # delete
        if not self.delete_based_on_entropies_step:
            self.df_entropies = pd.DataFrame(compute_entropies(self.df, self.df.columns))
            self.df_entropies = self.df_entropies.sort_values(by="entropy", ascending=False)
            col_with_null_entropies = list(self.df_entropies[self.df_entropies['entropy']==0]['col'].values)
            print(f"Il y a {len(col_with_null_entropies)} cols avec entropies nulles.")
            self.cols_to_delete.update(col_with_null_entropies)
            self.df = self.df.drop(col_with_null_entropies, axis=1)
            self.delete_based_on_entropies_step = True
        return self

    def run(self):
        self.mlflow.log_param("Input_data_rows", self.df.shape[0])
        self.mlflow.log_param("Input_data_features", self.df.shape[1])
        self.delete_colnan().cast_object_columns()
        self.mlflow.log_param("Output_data_rows", self.df.shape[0])
        self.mlflow.log_param("Output_data_features", self.df.shape[1])
        # logger un apercu du dataset
        mlflow_dataset_apercu = self.mlflow.data.from_pandas(
            self.df.head(5),
            # targets="target",  # we specify the target column
            name="DPE ENEDIS ADEME Dataset" # we specify the name of the dataset
        )
        self.mlflow.log_input(mlflow_dataset_apercu, context="cleaned_dataset")
