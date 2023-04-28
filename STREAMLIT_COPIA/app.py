import streamlit as st



st.set_page_config(page_title='Página de inicio ', 
                    page_icon='🐮', 
                    layout="centered", 
                    initial_sidebar_state="auto", 
                    menu_items=None)

col1, col2, col3 = st.columns(3)

with col1:
    st.write(' ')


with col2:
    st.image("imagenes/BASTO.jpeg")

with col3:
    st.write(' ')

st.title('Bienvenido a BASTÓ')

st.write('BASTÓ es un StartUp que apuesta por la transformación de la ganadería. A través del desarrollo de una cerca virtual dinámica que, a través de un collar inteligente, emite estímulos inocuos, cuidando el bienestar animal, contiene y arrea al ganado de un corral a otro gestionando un pastoreo eficiente, sustentable y de precisión.')


st.write('A través de esta página podemos visualizar los datos de GPS del ganado a lo largo de una serie de tiempo para observar el comportamiento en 4 momentos específicos del día: Madrugada, Mañana, Tarde y Noche de la siguiente forma:')


st.image('imagenes/GPS_potr.png')

st.write('Para consultar datos sobre el ganado de sus potreros, seleccione la pestaña Home, en donde se visualizará un despliegue de información general y particular sobre áreas deseadas.')

