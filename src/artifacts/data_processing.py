import warnings, os 
import pandas as pd

warnings.filterwarnings('ignore')

from utils.fonctions import get_entropy, compute_entropies, normalize_df_colnames


class DataPreprocessor:
    def __init__(self, df):
        self.nettoyage = Nettoyage(df)

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
        except Exception as e:
            print(f"Exception occured while saving data processed : {e}")


class Nettoyage:

    def __init__(self, df):
        self.df = normalize_df_colnames(df)
        self.cols_to_delete = set()
        self.df_entropies = None
        self.delete_colnan_step = False
        self.fillna_step = False
        self.cast_object_columns_step = False
        self.delete_based_on_entropies_step = False
        
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
        self.delete_colnan().cast_object_columns()
