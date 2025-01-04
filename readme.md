### Opendata University Challenge DPE

Last update : 23/12/2024

#### Documentation du projet 

- Source : [Opendata university challenge DPE (defis data gouv)](https://defis.data.gouv.fr/defis/diagnostics-de-performance-energetique).
- TBD : problématique, objectifs, livrables etc.. *(déjà fait - à compléter)*

#### Données
- Périmètre : PARIS 2018 
- Origine dataset : classe pour extraire les données croisées consommations ENEDIS - API BAN - DPE ADEME  
- Résultats des extracts par exploitation des API : exemples [ici](ressources/data/)

#### Utilisation

For the project you can make a virtual env if needed. 

- After cloning, in your favorite terminal/shell do :
```
pip install -r requirements.txt
```
To fetch data :
 - see [this exemple notebook](notebooks/1_database.ipynb).
 - data extraction perf exemple w/o ascyncio - mode unitaire : 
 ![img](docs/scope_paris_2018_200_min.png)


#### Perspectives : 
- Généraliser périmètre géographique France
- Optimiser la classe d'extraction : exple PARIS 2018 ~ 4H ~ 450k lignes au final
- *... (à compléter)*