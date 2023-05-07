import pandas as pd
import plotly.graph_objects as go
from funciones_app import data_devices, gps_data
import random

def lista_colores_ux_ui():
    colores = ["#007bff", "#28a745", "#dc3545", "#ffc107", "#17a2b8", "#6c757d", "#343a40","#343a40", "#007bff", "#28a745", "#dc3545", "#ffc107", "#17a2b8", "#6c757d", "#343a40", "#f8f9fa", "#007bff", "#dc3545", "#ffc107", "#17a2b8", "#6c757d", "#343a40", "#f8f9fa", "#343a40", "#28a745", "#dc3545", "#ffc107", "#17a2b8", "#6c757d", "#343a40", "#f8f9fa", "#343a40"]
    return colores

def random_color():
    """Genera un color cálido aleatorio en formato hexadecimal."""
    lista_colores= lista_colores_ux_ui()
    n = random.randint(2,len(lista_colores)-1)
    color =lista_colores[n]
    print(color)
    return color


mapbox_access_token = 'pk.eyJ1IjoibmVzdG9yMTYwOCIsImEiOiJjbGc5b2J2d3gwOHgwM2xwamd3cGE4cmExIn0.bPWyeRa73WyNqm1nBNJOvQ' 

def uni_graf(data,color,fig):

    fig.add_trace(go.Scattermapbox(
        lat=data.dataRowData_lat.values,
        lon=data.dataRowData_lng.values,
        mode='lines+markers',
        line=dict(
            width=2,
            color=color,
        ),
        marker=go.scattermapbox.Marker(
            size=8,
            color=color,
            symbol='circle'
        ),
        text=data.UUID.values
    ))
    return fig

def graf_aguada(data,fig):

    fig.add_trace(go.Scattermapbox(
    lat=data.dataRowData_lat.values,
    lon=data.dataRowData_lng.values,
    mode='markers',
    marker=dict(
        size=10,
        color='red',
    ),
    text= 'Aguada'
    ))
    return fig

def grafic_map(data, list_vacas, fig):
    colores=[]
    for i in list_vacas:
        color = random_color()
        colores.append(color)
        while color in colores:
            color =random_color()
        dta=data_devices(data, i )
        dta_gps= gps_data(dta)
        uni_graf(dta,color,fig)

    # fig.update_layout(
    #     mapbox=dict(
    #         style='satellite', # Estilo de mapa satelital
    #         accesstoken=mapbox_access_token,
    #         zoom=14, # Nivel de zoom inicial del mapa
    #         center=dict(lat=lat_orig,lon=lng_orig),
    #     ),
    #     showlegend=False
    # )
    return fig


