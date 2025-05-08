from contents import *


def main(selected_ville, selected_annee):
    st.title('Diagnostics de performance énergétique  -  Paris 2022')
    st.header('Présentation générale du projet')
    
    st.subheader('Contexte')
    st.markdown('''
Le changement climatique et la hausse des prix de l’énergie poussent à la sobriété énergétique. ​
Objectif politique : la neutralité carbone à l'horizon 2050, diminuer les GES des bâtiments d’ici 2030 
Rénovation énergétique est un levier économique clé car les bâtiments sont responsables d'une part importante des émissions de GES. ​
''')

    st.subheader('Problématique et objectifs')
    st.markdown(''' 
**Problématique :**
- Les estimations conventionnelles fournies dans les DPE reflètent-elles bien la réalité mesurée des consommations électriques ? 
- Et quelle est la variabilité restante due à l’hétérogénéité des modes de vie et taux d’occupation ?

**Objectifs :**
- étudier l’impact des caractéristiques du bâtiment ou des équipements (mode de chauffage, isolation, année de construction, etc.) sur la consommation électrique réelle
- modéliser la consommation électrique en fonction de la classe DPE, des habitudes de consommation et des caractéristiques des bâtiments, puis comparer avec la consommation réelle et analyser la part du delta expliquée par le DPE et celle expliquée par les habitudes
- se prononcer sur la conformité ou non des estimations de consommation présentées dans les DPE par rapport aux données réelles (consommation théorique vs consommation réelle)
- quantifier les gains potentiels liés à une amélioration de la performance énergétique d’un logement via une amélioration de la classe DPE (impact sur la consommation et éventuellement sur le prix, toutes choses égales par ailleurs)
- discuter l’impact des habitudes individuelles de consommation sur ces résultats, usage par usage : chauffage, eau chaude sanitaire, refroidissement, éclairage, autres
''')

    multi = '''Les travaux réalisés dans le cadre de ce projet permettront :

• D’éclairer les décisions de rénovation en quantifiant les gains potentiels associés à une amélioration de la performance énergétique d’un logement reflétée par une amélioration du DPE

• De valider la conformité au réel des estimations de consommation électrique présentées dans les DPE par des comparaisons aux données de consommations réelles et de quantifier la variabilité due aux comportements individuels.
    '''
    st.markdown(multi)
    
st.subheader("Qu'est-ce que le DPE ?")
st.markdown('''
Le Diagnostic de Performance Énergétique (DPE) est un document qui évalue la consommation énergétique d'un bâtiment et 
son impact sur l'environnement. Il attribue une étiquette allant de A (très performant) à G (très peu performant), 
permettant ainsi d'identifier les "passoires énergétiques" qui consomment beaucoup d'énergie 
et émettent de nombreux gaz à effet de serre.

**Enjeux du DPE :**
- Transition énergétique : Le DPE vise à réduire la consommation d'énergie des bâtiments et à diminuer les émissions de gaz à effet de serre, un enjeu crucial face au changement climatique. 
- Économie d'énergie : En informant les propriétaires et les locataires sur la performance énergétique de leur logement, le DPE incite à réaliser des travaux de rénovation pour améliorer l'efficacité énergétique, ce qui peut réduire les factures d'énergie. 
- Réglementation : Depuis 2023, la mise en location des logements classés G est interdite, et cette réglementation s’étendra aux logements notés F d'ici 2028.

**Avantages du DPE :**
- Information : Permet aux acquéreurs et locataires de faire des choix éclairés en matière de consommation énergétique. 
- Économie : Peut aider à réaliser des économies d'énergie et de coûts en encourageant les rénovations. 
- Valorisation du bien : Un bon DPE peut augmenter la valeur d'un bien immobilier.

**Inconvénients du DPE :**
- Méthode de calcul : Les calculs sont souvent basés sur des critères standardisés qui ne tiennent pas compte des comportements réels des occupants, ce qui peut rendre les estimations moins précises. 
- Coût des diagnostics : La réalisation du DPE a un coût qui peut être un frein pour certains propriétaires.

**Qui réalise le DPE ? :**
Le DPE doit être réalisé par des professionnels certifiés, appelés diagnostiqueurs immobiliers. Ils suivent des formations spécifiques et sont soumis à des règles strictes pour garantir la fiabilité des diagnostics. 

**Méthodes de calcul :**
- Méthode 3CL
- Méthode des factures

**Réformes du DPE :**
- 2011 : Amélioration de la méthode de calcul pour renforcer la fiabilité des diagnostics. 
- 2021 : Introduction d'une nouvelle méthode de calcul et d'étiquettes opposables. Le DPE est devenu un document ayant valeur juridique. 
- 2023 et au-delà : De nouvelles réformes sont prévues pour améliorer la précision du DPE et renforcer les exigences réglementaires. 
''')