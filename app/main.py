from contents import *
from contents import page1, page2, page3, page4

# st.set_option('deprecation.showPyplotGlobalUse', False)
# st.set_page_config(layout="wide")

pages = {
    " 1 - Présentation générale": page1.main,
    " 2 - Analyse exploratoire": page2.main,
    " 3 - Modèles": page3.main,
    " 4 - Démo": page4.main,
}

st.sidebar.title('Navigation')
p = st.sidebar.radio('Aller à  ', list(pages.keys()))

st.sidebar.markdown("-------------------")

st.sidebar.header('Filtres')
ville_filter = st.sidebar.selectbox('Ville', ['Paris'])
annee_filter = st.sidebar.selectbox('Année', ['2022'])

pages[p](ville_filter, annee_filter)