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

# st.write('BASTÓ es un StartUp que apuesta por la transformación de la ganadería. A través del desarrollo de una cerca virtual dinámica que, a través de un collar inteligente, emite estímulos inocuos, cuidando el bienestar animal, contiene y arrea al ganado de un corral a otro gestionando un pastoreo eficiente, sustentable y de precisión.')


st.write('A través de esta página podemos visualizar los datos de GPS del ganado y de comportamiento a lo largo de un periodo de tiempo específico, así como **la distribución de tiempo entre las siguientes actividades:**')


st.write('**Pastoreo**, **Rumia**, **Descanso**,  **Bebiendo**')


st.write('En la pestaña de "home" se podrá encontrar la información filtrada como en la API que se desarrolló a lo largo de este proyecto')


st.image('imagenes/GPS_potr.png')


