import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from funciones_app import filter_time_day,dataframe_interview_vaca,data_devices,week_data_filter, filter_area_perimetro
from conect_datarows import setle_clean,selec_setle,obtener_fecha_inicio_fin, df_gps,setle_list
from prueba import conducta_vaca_periodo
from suport_st import grafic_map,mapbox_access_token
import plotly.express as px

st.image('imagenes/Header_bastó.jpeg')

# st.title('Información general')

setle= setle_list()# arroja dataframe arreglado de setle---


# st.title('Información de Potrero de interés')


# select_sl= st.selectbox('Seleccione un asentamiento a evaluar:',setle.name.unique())
# nombre= setle[setle.name==select_sl]._id.values[0]
# print(nombre)
# elec_setle= selec_setle(setle,nombre) # arroja dataframe pequeño de un solo dato del asentamiento---

# on_perimetro=filter_area_perimetro(df_gps,elec_setle.latitud_c, elec_setle.longitud_c, elec_setle.hectares)# arroja dataframe---
# uuid_devis = on_perimetro.UUID.unique()

# clicked_3 = st.button("Conteo dispositivo")
# if clicked_3:
#     st.write(
#         f"En esta área hay: {len(uuid_devis)} collares que proveen datos de GPS de un total de 518 vacas en total"
#     )

# st.write('A continuación se pueden observar los diferentes perímetros a consultar a partir de los datos proveídos:')


# setle= setle_list()# arroja dataframe arreglado de setle---


st.dataframe(setle,use_container_width=True)


st.write('Favor de aplicar los filtros necesarios para su consulta:')

# select_sl= st.selectbox('Seleccione un asentamiento',setle.name.unique())
nombre= setle[setle.name=='La Florida']._id.values[0]

elec_setle= setle[setle.name=='La Florida'] # arroja dataframe pequeño de un solo dato del asentamiento---
on_perimetro=filter_area_perimetro(df_gps,elec_setle.latitud_c, elec_setle.longitud_c, elec_setle.hectares)# arroja dataframe---

if on_perimetro.shape[0]!=0:
    uuid_devis = on_perimetro.UUID.unique()

    select=st.selectbox("Ahora seleccione un collar",uuid_devis)
    dt_vaca=  data_devices(on_perimetro,select)
    dt_vaca.createdAt= pd.to_datetime(dt_vaca.createdAt)

    data_week= dt_vaca.groupby(['UUID',dt_vaca.createdAt.dt.week]).agg({'createdAt':'count'}).rename(columns={'createdAt':'count_register'})
    data_week=data_week.reset_index()


st.write('Visualización de los registros obtenidos a lo largo del tiempo de ese collar en esa locaclización en específica:')



st.write('Ahora puede observar una semana en específica con el menú siguiente:')

if int(data_week['createdAt'].min())!= int(data_week['createdAt'].max()):
        fig= px.bar( data_week,x='createdAt',y='count_register')
        st.markdown('## Cantidad de registro por Semana')
        st.plotly_chart(fig,use_container_width=True)
        week= st.slider('Selecione semana',int(data_week['createdAt'].min()) ,int(data_week['createdAt'].max()) )

st.write('En esa semana específica, puede visualizar los datos de un momento específico del día y sus datos de ese collar en específico:')

time_week= week_data_filter(dt_vaca,week)

sep_time=time_week.groupby(time_week.createdAt.dt.date).agg({'UUID':'count'}).rename(columns={'UUID':'count_register'}).reset_index().rename(columns={'createdAt':'day'})
sep_time.day= pd.to_datetime(sep_time.day)
day=sep_time.day.dt.date.values

fig=px.bar(sep_time,x=sep_time.day.dt.day_name(), y=sep_time.count_register)
st.plotly_chart(fig,use_container_width=True) 




# fi_time= filter_time_day(time_week,time_day)

# if int(data_week['createdAt'].min())!= int(data_week['createdAt'].max()):
#         fig= px.bar( week,x='createdAt',y='count_register')
#         st.markdown('## Cantidad de registro por día')
#         st.plotly_chart(fig,use_container_width=True)
#         dia= st.slider('Selecione dia',int(week['createdAt'].min()) ,int(week['createdAt'].max()) )


st.markdown('***')
st.markdown('## Cantidad de registro por dia')

day_select=st.select_slider('Seleccionar dia',options=day)


sep_time=time_week[time_week['createdAt'].dt.date ==day_select].groupby(time_week.createdAt.dt.date).agg({'UUID':'count'}).rename(columns={'UUID':'count_register'}).reset_index().rename(columns={'createdAt':'day'})
sep_time.day= pd.to_datetime(sep_time.day)
day=sep_time.day.dt.date.values

# fig=px.bar(sep_time,x=sep_time.day.dt.day_name(), y=sep_time.count_register)
# st.plotly_chart(fig,use_container_width=True) 

try:
    date_week= obtener_fecha_inicio_fin(time_week.iloc[-1][['createdAt']].values[0])
    st.subheader(f'Fecha de Inicio: {date_week[0]}')
    st.subheader(f'Fecha de fin: {date_week[1]}')
except IndexError:
    st.warning('No hay datos para estos momento del dia')


# dia = range(week)
# time_day=st.select_slider('Selecione el dia',options=dia)


fi_time= time_week[time_week['createdAt'].dt.date ==day_select]

# st.subheader(day.upper())



val_vaca= dataframe_interview_vaca(fi_time)

st.dataframe(val_vaca,use_container_width=True)


if st.button('Recorrido en Mapa') or fi_time.shape[0]==1:
        fig = go.Figure()
        grafic_map(fi_time,[select], fi_time.iloc[0]['dataRowData_lat'], fi_time.iloc[0]['dataRowData_lng'], fig)
        fig.update_layout(
            mapbox=dict(
                style='satellite', # Estilo de mapa satelital
                accesstoken=mapbox_access_token,
                zoom=12, # Nivel de zoom inicial del mapa
                center=dict(lat=elec_setle.latitud_c.values[0] , lon=elec_setle.longitud_c.values[0]),
            ),
            showlegend=False
        )
        st.plotly_chart(fig)

if fi_time.shape[0]!=0:
    try:
        if fi_time.shape[0]>1:
                mean_dist, dist_sum =val_vaca[['distancia']].mean().round(3),val_vaca['distancia'].sum().round(3)
                sum_tim, time_mean= val_vaca['tiempo'].sum().round(3),val_vaca['tiempo'].mean().round(3)
                velo_mean=val_vaca['tiempo'].mean().round(3)
                st.markdown(f'Movimiento promedio durante **{day_select}** fue  **{mean_dist.values[0]}**km')
                st.markdown(f'Distancia recorrida: **{dist_sum}** km')
                st.markdown(f'Tiempo: {sum_tim} ')
                st.markdown('***')
                st.subheader('Variaciones de movimiento y distancia')
                fig=px.area(val_vaca, x=val_vaca['point_ini'], y= val_vaca['distancia'],)
                st.plotly_chart(fig,use_container_width=True)
                st.markdown('***')
                st.subheader('Alteracion de velocidad')
                fig=px.area(val_vaca, x=val_vaca['point_ini'],y=val_vaca['velocidad'])
                st.plotly_chart(fig,use_container_width=True) 
                st.markdown(f'* Velocidad promedio **{velo_mean}** k/h')
                st.markdown('***')
                
                st.subheader('Variaciones de Tiempo ')
                fig=px.area(val_vaca, x=val_vaca['point_ini'], y= val_vaca['tiempo'])
                st.plotly_chart(fig,use_container_width=True) 
                st.markdown(f'* Tiempo promedio:  **{time_mean}** hrs')
        else:
            st.warning('No hay registro con estos parametros')
    except AttributeError:
        st.table(fi_time[['dataRowData_lng','dataRowData_lat' ]])
        
        
        
    
        
    tabla_datos,tabla_resumen,tabla_diag= conducta_vaca_periodo(time_week, on_perimetro,select, select_sl ,date_week[0],date_week[1])# ACAAA ESTA CREADO EL DATAFRAME CON LOS VALORES
    
    
    
    
    
    

    st.markdown('***')

    st.write('Teniendo en cuenta la distancia, la aceleración y el tiempo que varía de un punto a otro, se creo un modelo de K-means el que nos arroja un agrupamiento de las actividades de la vaca que se pueden visualizar así:')    

    #st.dataframe(tabla_datos,use_container_width=True)
    
    st.dataframe(tabla_resumen,use_container_width=True)

    st.write('Dados los parámetros óptimos, en la siguiente tabla se puede concluir que el la calidad de la distribución del tiempo que dedicó a cada actividad')

    st.dataframe(tabla_diag, use_container_width =True)

else:
    st.warning('Lugar sin dato')