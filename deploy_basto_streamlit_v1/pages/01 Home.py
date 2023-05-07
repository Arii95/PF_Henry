import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from funciones_app import dataframe_interview_vaca,data_devices, filter_area_perimetro,transform,get_range_week,select_data_by_dates,count_day_hour
from conect_datarows import obtener_fecha_inicio_fin, df_gps, setle_list,agregar_iths,cosa
from prueba import conducta_vaca_periodo,agua_click
from suport_st import grafic_map,mapbox_access_token
import plotly.express as px

import datetime


st.title('BASTÓ Ganado Inteligente')
st.image('imagenes/Header_bastó.jpeg')


setle= setle_list()# arroja dataframe arreglado de setle---


st.markdown('***')
st.subheader('Asentamientos con Registros')
st.dataframe(setle[['name', 'hectares', 'latitud_c', 'longitud_c']],use_container_width=True)


st.write('Favor de aplicar los filtros necesarios para su consulta:')

# seleccion de asentamiento 
select_sl= st.selectbox('Seleccione un asentamiento',setle.name.unique())


# FILTRADO DE DATAFRAME POR ASENTAMIENTO-------------------------------------------------------

on_perimetro=filter_area_perimetro(df_gps,select_sl)# arroja dataframe---

# Control de error por dataframe vacio----*******
if on_perimetro.shape[0]!=0:
    uuid_devis = on_perimetro.UUID.unique()
# Seleccion de collar --------------------
    select=st.selectbox("Ahora seleccione un collar",uuid_devis)
    
    # filtrado de dataframe por collar especifico-------+++++++++
    dt_vaca=  data_devices(on_perimetro,select)
    dt_vaca.createdAt= pd.to_datetime(dt_vaca.createdAt)
    
    # control de error por collar vacio----------*********
    if dt_vaca.shape[0]!=0:
        st.write(f'{dt_vaca.createdAt.dt.year.unique()}')
        data_week= dt_vaca['createdAt'].groupby(dt_vaca.createdAt.dt.strftime('%U')).aggregate(['count']).rename(columns={'count':'count_register'})
        data_week=data_week.reset_index()
        data_week.createdAt = data_week.createdAt.apply(lambda x : int(x))
        


        if int(data_week['createdAt'].min())!= int(data_week['createdAt'].max()):
            st.write('Ahora puede observar una semana en específica con el menú siguiente:')
            fig= px.bar( data_week,x=data_week['createdAt'],y=data_week['count_register'])
            st.markdown('## Cantidad de registro por Semana')
            st.plotly_chart(fig,use_container_width=True)
            week= st.slider('Selecione semana',int(data_week['createdAt'].min()) ,int(data_week['createdAt'].max()) )
        else:
            st.write('El collar posee solo registros de una semana, estos son:')
            week= data_week['createdAt'].unique()

        inici_semana, fin_semena= get_range_week(dt_vaca.createdAt.dt.year.unique()[0],int(week))
        #st.write(week,'-',inici_semana,'->',fin_semena)
        time_week= select_data_by_dates(dt_vaca,inici_semana,fin_semena) # SE RELAIZA EL DATAFRAME POR LA SEMANA 
        #st.write(f'{time_week.shape}')
        
        if time_week.shape[0]!=0:
            sep_time=time_week['createdAt'].groupby(dt_vaca.createdAt.dt.date).aggregate(['count']).rename(columns={'count':'count_register'}).reset_index()
            sep_time.createdAt= pd.to_datetime(sep_time.createdAt)
            sep_time = sep_time.sort_values('createdAt',ascending=True)
            day=sep_time.createdAt.dt.date



            st.write('En esa semana específica, puede visualizar los datos de un momento específico del día y sus datos de ese collar en específico:')
            fig=px.bar(sep_time,x=sep_time.createdAt.dt.day_name(), y=sep_time.count_register)
            st.plotly_chart(fig,use_container_width=True) 


            st.markdown('***')
    #  control de error DIA SIN REGISTRO --------------------*********    
            if len(day) != 1:
                st.markdown('## Cantidad de registro por dia')
                day_select=st.select_slider('Seleccionar dia',options=day)
                sep_time=time_week.groupby(time_week.createdAt.dt.date).agg({'UUID':'count'}).rename(columns={'UUID':'count_register'}).reset_index().rename(columns={'createdAt':'day'})
                sep_time.day= pd.to_datetime(sep_time.day)
                day=sep_time.day.dt.date.values
                
                date_week= obtener_fecha_inicio_fin(time_week.iloc[-1][['createdAt']].values[0])
                st.subheader(f'Fecha de Inicio: {inici_semana}')
                st.subheader(f'Fecha de fin: {fin_semena}')
                

                fi_time= time_week[time_week['createdAt'].dt.date == pd.to_datetime(day_select).date()]
                #st.write(f'{fi_time.shape}') CONTROLADOR 
            else:
                st.markdown('## Unico dia que tiene registros')
                date_week= obtener_fecha_inicio_fin(time_week.iloc[-1][['createdAt']].values[0])
                st.subheader(f'Fecha de Inicio: {inici_semana}')
                st.subheader(f'Fecha de fin: {fin_semena}')
                day_select= day[0]
                fi_time= time_week[time_week['createdAt'].dt.date == pd.to_datetime(day_select).date()]

    #  control de error DIA SIN REGISTRO --------------------*********
            if fi_time.shape[0]!=0:

                val_vaca= dataframe_interview_vaca(fi_time)
                val_vaca= agregar_iths(val_vaca,setle[setle.name==select_sl]._id.values[0])
                st.dataframe(val_vaca,use_container_width=True)
            else:
                st.warning('Dia sin registros') 

    #  control de error DIA CON MAS DE UN REGISTRO-------------------*********
            if fi_time.shape[0] > 1:
                mean_dist, dist_sum =val_vaca[['distancia']].mean().round(3),val_vaca[['distancia']].sum().round(3)
                #st.write(f'{val_vaca[['tiempo']].sum()} -- {val_vaca[['tiempo']].mean()}')
                sum_tim, time_mean= val_vaca[['tiempo']].sum().round(3), val_vaca[['tiempo']].mean().round(3)
                velo_mean=val_vaca[['tiempo']].mean().round(3)
                st.markdown(f'Movimiento promedio durante **{day_select}** fue  **{mean_dist.values[0]}** Km')
                st.markdown(f'Distancia recorrida: **{dist_sum.values[0]}** km')
                st.markdown(f'Tiempo: **{cosa(sum_tim.values[0])}** ')
                
# PRESENTACION DE MAPA--------------------------------------------------
                try: 
                    st.success('Podra visualizar el recorrido del bovino en un mapa satelital')
                    if st.button('Recorrido en Mapa') or fi_time.shape[0]==1:
                        fig = go.Figure()
                        grafic_map(fi_time,[select], fig)
                        
                        fig.update_layout(
                            mapbox=dict(
                                style='satellite', # Estilo de mapa satelital
                                accesstoken=mapbox_access_token,
                                zoom=12, # Nivel de zoom inicial del mapa
                                center=dict(lat=fi_time.iloc[-1]['dataRowData_lat'] , lon= fi_time.iloc[-1]['dataRowData_lng']),
                            ),
                            showlegend=False
                        )
                        st.plotly_chart(fig)
                except NameError or IndexError:
                    pass
#-----------------------------------------------------------------------------
                st.markdown('***')
                 #  GRAFICO CORESPONDIENTE A VARIACIONO DE MOVIMIENTO-------------------++++++++ 
                st.subheader('Variaciones de movimiento y distancia')
                st.markdown(f'* Valor corespondiente al dia {day_select}')
                fig=px.area(val_vaca, x=val_vaca['point_ini'], y= val_vaca['distancia'],)
                st.plotly_chart(fig,use_container_width=True)
                
                 # VECES QUE SE VA A LA AGUADA AEN EL DIA-----------+++++++++++++
                st.subheader('Veces que se va a las aguadas en el dia marcado')
                agua= agua_click(df_gps, select, day_select, setle[setle.name==select_sl]._id.values[0])
                if agua.shape[0] != 0:
                        agua= agua.drop(columns=['geometry'])
                        veces_dia= agua.groupby([agua['createdAt'].dt.hour]).agg({'UUID': 'count'})
                        veces_dia= veces_dia.reset_index().rename(columns= {'createdAt':'Hora', 'UUID':'Conteo'})
                        veces_dia= veces_dia.set_index('Hora')
                        st.dataframe(veces_dia, use_container_width=True)
                else:
                    st.warning('No hay registros de la aguada')
                    
                st.markdown('***')
                
                 #  GRAFICO CORESPONDIENTE A VELOCIDAD-------------------++++++++ 
                st.subheader('Alteracion de velocidad')
                st.markdown(f'* Valor corespondiente al dia {day_select}')
                fig=px.area(val_vaca, x=val_vaca['point_ini'],y=val_vaca['velocidad'])
                st.plotly_chart(fig,use_container_width=True) 
                st.markdown(f'* Velocidad promedio **{velo_mean.values[0]}** k/h')
                
                st.markdown('***')
                 #  GRAFICO CORESPONDIENTE A TIEMPO-------------------++++++++ 
                st.subheader('Variaciones de Tiempo ')
                st.markdown(f'* Valor corespondiente al dia {day_select}')
                fig=px.area(val_vaca, x=val_vaca['point_ini'], y= val_vaca['tiempo'])
                st.plotly_chart(fig,use_container_width=True) 
                st.markdown(f'* Tiempo promedio:  **{cosa(time_mean.values[0])}** hrs')
                

                tabla_datos,tabla_resumen,tabla_diag= conducta_vaca_periodo(time_week, df_gps,select, select_sl ,inici_semana, fin_semena)# ACAAA ESTA CREADO EL DATAFRAME CON LOS 
                    


                st.markdown('***')

                st.write('Teniendo en cuenta la distancia, la aceleración y el tiempo que varía de un punto a otro, se creo un modelo de K-means el que nos arroja un agrupamiento de las actividades de la vaca que se pueden visualizar así:')    


                st.subheader(' Tiempo acumulado por actividad ')

                st.dataframe(tabla_resumen,use_container_width=True)


                tabla_resumen[['rumiando','pastando','durmiendo','bebiendo']]= tabla_resumen[['rumiando','pastando','durmiendo','bebiendo']].applymap(lambda x: transform(x))
                fig=px.line(tabla_resumen[['rumiando','pastando', 'durmiendo', 'bebiendo']].transpose())
                st.plotly_chart(fig,use_container_width=True)


                st.subheader(' Tabla de diagnóstico ')

                st.write('Dados los parámetros óptimos, en la siguiente tabla se puede concluir que el la calidad de la distribución del tiempo que dedicó a cada actividad')
                st.dataframe(tabla_diag, use_container_width =True)
            else:
                st.table(fi_time[['dataRowData_lng','dataRowData_lat' ]])

        else:
            st.warning('No hay datos para esta semana cambie la semana seleccionada')

    else:
        st.warning('Collar sin Registros')
else:
    st.warning('Lugar sin dato')







