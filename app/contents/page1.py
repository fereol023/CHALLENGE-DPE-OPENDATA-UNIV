from contents import *


def main():
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
- modéliser la consommation électrique des logements à partir de données de consommation réelles et de données de DPE et évaluer l'importance des DPE
- à compléter
''')

    multi = '''Les travaux réalisés dans le cadre de ce projet permettront :

•	D’éclairer les décisions de rénovation en quantifiant les gains potentiels associés à une amélioration de la performance énergétique d’un logement reflétée par une amélioration du DPE

•	De valider la conformité au réel des estimations de consommation électrique présentées dans les DPE par des comparaisons aux données de consommations réelles et de quantifier la variabilité due aux comportements individuels.
    '''
    st.markdown(multi)

    st.header('Modèle de consommation électrique')
    