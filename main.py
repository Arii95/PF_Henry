#Importo librerias
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import plotly.graph_objects as go
import numpy as np
import seaborn as sns
import pandas as pd
import geopandas as gpd
import datetime
import string
import random
import pymongo
import folium
import math
import json
from sklearn.ensemble import IsolationForest
from math import sin, cos, sqrt, atan2, radians
from pymongo import MongoClient
from typing import Union,List, Dict, Any, Tuple, Optional
from shapely.geometry import Point
from geopy.distance import great_circle
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder


# Crear una conexión a una instancia de MongoDB en la dirección 'localhost:27017'.
data_mongo: MongoClient = pymongo.MongoClient('localhost', 27017)
# Seleccionar una base de datos existente o crear una nueva llamada 'test'.
db: pymongo.database.Database = data_mongo['test']
# Seleccionar una colección de la base de datos llamada 'datarows'.
rows: pymongo.collection.Collection = db['datarows']

# Obtener los n documentos de la colección y convertirlos a un DataFrame de Pandas.
drow1: pd.DataFrame = pd.json_normalize(list(rows.find()[:150000]), sep='_')
drow2: pd.DataFrame = pd.json_normalize(list(rows.find()[150000:300000]), sep='_')
drow3: pd.DataFrame = pd.json_normalize(list(rows.find()[300000:450000]), sep='_')
drow4: pd.DataFrame = pd.json_normalize(list(rows.find()[450000:600000]), sep='_')
drow5: pd.DataFrame = pd.json_normalize(list(rows.find()[600000:750000]), sep='_')
drow6: pd.DataFrame = pd.json_normalize(list(rows.find()[750000:900000]), sep='_')
drow7: pd.DataFrame = pd.json_normalize(list(rows.find()[900000:1050000]), sep='_')
drow8: pd.DataFrame = pd.json_normalize(list(rows.find()[1050000:1200000]), sep='_')
drow9: pd.DataFrame = pd.json_normalize(list(rows.find()[1200000:]), sep='_')

# Concatenar todos los DataFrames en un solo DataFrame.
df_row: pd.DataFrame = pd.concat([drow1, drow2, drow3, drow4, drow5, drow6, drow7, drow8, drow9], axis=0)
# Se asigna el tipo de dato 'str' a la columna '_id' del DataFrame 'df_row'.
df_row._id = df_row._id.astype(str)


def filter_data_types(df_row: pd.DataFrame) -> tuple:
    """
    Se seleccionan las filas del DataFrame original donde el valor de la columna 'dataRowType' es 'GPS', 
    'dataRowType' es 'BEACON' y 'dataRowType' es 'BATTERY' y se devuelve una tupla con los tres DataFrames filtrados
    
    Parámetro:
    -----------
    - df_row: DataFrame que contiene los datos a filtrar.

    Retorna:
    --------
    - Diferentes DataFrames, cada uno correspondiente al tipo de origen de dato indicado.
    """

    # Se cargan los valores en funcion de condicion de equivalencia.
    data_gps = df_row[df_row.dataRowType == 'GPS']
    data_beacon = df_row[df_row.dataRowType == 'BEACON']
    data_battery = df_row[df_row.dataRowType == 'BATTERY']
    return (data_gps, data_beacon, data_battery)

data_gps,data_beacon,data_battery = filter_data_types(df_row)
# Se seleccionan las columnas deseadas de cada futuro DataFrame .
df_gps=data_gps[['UUID','dataRowType','createdAt','updatedAt','dataRowData_lat','dataRowData_lng','dataRowData_gpsAlt','dataRowData_gpsVel','dataRowData_gpsFixed']]
df_bate=data_battery[['UUID','dataRowType','createdAt','updatedAt','dataRowData_timestamp','dataRowData_battery']]
df_beacon=data_beacon[['UUID','dataRowType','createdAt','updatedAt','dataRowData_timestamp','dataRowData_mac','dataRowData_battery','dataRowData_temperature','dataRowData_rssi','dataRowData_accelerometer']]


def data_devices(data: pd.DataFrame, uuid: str) -> pd.DataFrame:
    """
    Filtra los datos de un DataFrame que corresponden a un dispositivo específico
    y elimina las filas con valores faltantes en la columna dataRowData_lat.

    Parámetros:
    -----------
    - data: DataFrame que contiene los datos a filtrar.
    - uuid: string que corresponde al identificador único del dispositivo a filtrar.

    Retorna:
    --------
    - Un DataFrame que contiene solo los datos del dispositivo especificado, sin valores faltantes en dataRowData_lat.
    """
    data = data[data.UUID == uuid]
    data.drop(data[data.dataRowData_lat.isna()].index, inplace=True)
    data.reset_index()
    return data

def gps_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Selecciona solo las columnas dataRowData_lat y dataRowData_lng de un DataFrame y
    elimina las filas con valores faltantes en alguna de estas columnas.

    Parámetros:
    -----------
    - data: DataFrame que contiene los datos de GPS a seleccionar.

    Retorna:
    --------
    - Un DataFrame que contiene solo las columnas dataRowData_lat y dataRowData_lng, sin valores faltantes.
    """
    gps = data[['dataRowData_lat', 'dataRowData_lng']]
    gps = gps.dropna()
    return gps

def perimetro_aprox(hectarea: float) -> float:
    """
    Calcula el perímetro aproximado de un terreno a partir de su área en hectáreas.
    
    Parámetros:
    -----------
    - hectarea: área del terreno en hectáreas
    
    Retorna:
    -----------
    - perim: perímetro aproximado del terreno en metros
    """
    hect = hectarea  # Asignamos el valor del parámetro hectarea a la variable hect
    lado = math.sqrt(hect) * 10  # Calculamos la longitud del lado de un cuadrado cuya área es igual a hect y multiplicamos por 10
    perim = lado * 4  # Calculamos el perímetro del cuadrado multiplicando la longitud del lado por 4

    return perim

def area_perimetro(data: pd.DataFrame, latitud: float, longitud: float, hectareas: float) -> gpd.GeoDataFrame:
    """
    Devuelve una GeoDataFrame con las geometrías de los terrenos que se encuentran en el perímetro de un círculo
    centrado en las coordenadas dadas y con un radio que corresponde al perímetro aproximado de un terreno
    de la misma área que se especifica.
    
    Parámetros:
    -----------
    - latitud: latitud del centro del círculo
    - longitud: longitud del centro del círculo
    - hectareas: área del terreno en hectáreas
    
    Retorna:
    -----------
    - on_perimetro: GeoDataFrame con las geometrías de los terrenos que se encuentran en el perímetro del círculo
    """
    gdf= gpd.GeoDataFrame(data,crs='EPSG:4326',geometry=gpd.points_from_xy(data.dataRowData_lng,data.dataRowData_lat))

    setle_lat = latitud # Asignamos el valor del parámetro latitud a la variable setle_lat
    setle_lng = longitud # Asignamos el valor del parámetro longitud a la variable setle_lng
    punto_referencia = Point(setle_lng, setle_lat) # Creamos un punto de referencia con las coordenadas setle_lat y setle_lng
    per_kilo = perimetro_aprox(hectareas) # Calculamos el perímetro en metros aproximado a partir del área en hectáreas
    circulo = punto_referencia.buffer(per_kilo/111.32) # Creamos un círculo con el radio igual al perímetro en metros, dividido entre 111.32 km, aproximando a 1 grado en el ecuador
    on_perimetro = gdf[gdf.geometry.within(circulo)] # Filtramos el GeoDataFrame gdf para obtener los puntos dentro del círculo creado anteriormente.
    return on_perimetro

def filtro_finca(dada: pd.DataFrame,lat: float, long: float, hect: int) -> pd.DataFrame:
    """
    Función que filtra el DataFrame resultante de la función 'area_perimetro' eliminando las filas con valores faltantes.

    Parametros:
    -----------
    - lat (float): latitud de la finca.
    - long (float): longitud de la finca.
    - hect (int): área en hectáreas de la finca.

    Retorno:
    -----------
    - df_finca (pd.DataFrame): DataFrame que contiene la información de la finca filtrada.
    """

    df_finca: pd.DataFrame = area_perimetro(dada,lat, long, hect)
    return df_finca

def select_data_by_date(df: pd.DataFrame, fecha: str) -> pd.DataFrame:
    """
    Selecciona las filas de un DataFrame correspondientes a una fecha específica.
    
    Parametros:
    - df: DataFrame de pandas que contiene la columna "createdAt".
    - fecha: Fecha en formato de cadena, en el formato 'YYYY-MM-DD'.
    
    Returno:
    - DataFrame de pandas que contiene solo las filas correspondientes a la fecha especificada.
    """
    
    # Convertir la columna "createdAt" en un objeto datetime
    df['createdAt'] = pd.to_datetime(df['createdAt'])

    # Seleccionar solo las filas correspondientes a la fecha especificada
    fecha_deseada = pd.to_datetime(fecha).date()
    nuevo_df = df.loc[df['createdAt'].dt.date == fecha_deseada]

    return nuevo_df

def dataframe_interview_vaca(data: pd.DataFrame) -> pd.DataFrame:
    """
    Función que procesa un DataFrame de datos de GPS para calcular la distancia recorrida, la velocidad promedio y el tiempo
    de recorrido entre cada par de puntos consecutivos. Además, agrega una columna con la relación de velocidad entre puntos 
    consecutivos.

    Parametros:
    -----------
    - DataFrame de datos de GPS con columnas 'createdAt', 'dataRowData_lat', 'dataRowData_lng' y 'dataRowData_gpsVel'
    
    Retorno:
    -----------
    - DataFrame con las columnas 'point_ini', 'point_next', 'interval_time', 'distancia', 'velocidad', 'tiempo' y 'charge_vel'
    """

    data_dis: List[float] = []
    data_vel: List[float] = []
    data_time: List[float] = []
    data_inter: List[int] = []
    data_in: List[str] = []
    data_fin: List[str] = []
    data_alg: List[float] = []

    for i in range(0, data.shape[0] + 1):
        try:
            # Calcula la distancia en kilómetros entre el punto i y el siguiente punto
            dista_km = great_circle(tuple(data.iloc[i][['dataRowData_lat', 'dataRowData_lng']].values), 
                                    tuple(data.iloc[i + 1][['dataRowData_lat', 'dataRowData_lng']].values)).kilometers
            data_in.append(data.iloc[i][['createdAt']].values[0])
            data_fin.append(data.iloc[i + 1][['createdAt']].values[0])
            # Calcula la diferencia en horas entre la hora de los dos puntos
            interval = int(data.iloc[i][['createdAt']].values[0].strftime('%H')) - int(data.iloc[i + 1][['createdAt']].values[0].strftime('%H'))
            data_inter.append(interval)
            
            # Calcula la relación de velocidad entre puntos consecutivos
            if i == 0:
                data_var = data.iloc[i]['dataRowData_gpsVel']
                data_alg.append(data_var)
            else:
                data_var = data.iloc[i + 1]['dataRowData_gpsVel'] - data.iloc[i - 1]['dataRowData_gpsVel']
                data_alg.append(data_var)

            # Agrega la distancia, velocidad y tiempo entre los puntos consecutivos a sus respectivas listas
            if dista_km <= 8.:
                data_dis.append(round(dista_km, 3))
            
            if data.iloc[i].dataRowData_gpsVel:
                data_vel.append(round(data.iloc[i].dataRowData_gpsVel, 3))
                data_time.append(round(dista_km / data.iloc[i].dataRowData_gpsVel, 3))
            else:
                data_time.append(round(dista_km / pd.Series(data_vel).mean().round(3), 3))

        except IndexError:
            pass

    # Crea un DataFrame con las listas de datos y los nombres de las columnas correspondientes
    df = list(zip(data_in, data_fin, data_inter, data_dis, data_vel, data_time, data_alg))
    df = pd.DataFrame(df, columns=['point_ini', 'point_next', 'interval_time', 'distancia', 'velocidad', 'tiempo', 'charge_vel'])
    
    return df

def setle_clean(select):
    de= db['settlements']
    obj= de.find_one({'name':select})
    df_setle= pd.json_normalize(obj,sep='')
    df_setle['latitud_c']=df_setle.centralPoint.apply(lambda x: x[0]['lat'] if 'lat' in x[0] else None)
    df_setle['longitud_c']=df_setle.centralPoint.apply(lambda x: x[0]['lng'] if 'lng' in x[0] else None)
    setle_n = df_setle[['_id','hectares','registerNumber','headsCount','name','latitud_c','longitud_c']]
    return setle_n

df_gps = gps_data(df_gps)
df_gps = filtro_finca(df_row,-34.164999,-64.070010,1.0900)
df_gps = df_gps.sort_values(by='UUID')
df_gps = data_devices(df_gps,"0004A30B00F89C5D")
df_gps = df_gps.sort_values(by='createdAt')
df_gps = select_data_by_date(df_gps,'2023-02-18')
df_gps = dataframe_interview_vaca(df_gps)

#Creo una instancia de FastAPI
app = FastAPI()

#---- PRESENTACIÓN--------

@app.get("/")
def bienvenida():
    return "Bienvenidos a Bastó - Ganaderia Inteligente"

@app.get("/menu")
def menu():
    return "Las funciones que encontrara son las siguientes: (1) get_max_duration (2) get_score_count (3) get_count_platform (4) get_actor"



#---------- Queries-----
#Primer Consulta: Informacion propia de una finca.
@app.get("/informacion_por_finca/{nombre}")
def informacion_por_finca(nombre: str):
    
    data_finca = setle_clean(nombre)
    df_gps = filtro_finca(df_row,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])

    return df_gps

#Segunda Consulta: Informacion de una vaca especifica en una finca
@app.get("/informacion_una_vaca_por_finca/{nombre}/{id}")
def informacion_una_vaca_por_finca(nombre : str, id : str):
   
    data_finca = setle_clean(nombre)
    df_gps = filtro_finca(df_row,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    df_gps = df_gps.sort_values(by='UUID')
    df_gps = data_devices(df_gps,id)

    return df_gps


#Tercer Consulta: Informacion del dia especifico, de una vaca en una finca 
@app.get("/informacion_por_un_dia_una_vaca_por_finca/{platform}/{id}/{fecha}")
def informacion_por_un_dia_una_vaca_por_finca(nombre : str, id : str, fecha: str):
    
    data_finca = setle_clean(nombre)
    df_gps = filtro_finca(df_row,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    df_gps = df_gps.sort_values(by='UUID')
    df_gps = data_devices(df_gps,id)
    df_gps = df_gps.sort_values(by='createdAt')
    df_gps = select_data_by_date(df_gps,fecha)

    return df_gps


@app.get("/rutina_vaca/{nombre}/{id}/{fecha}")
def rutina_vaquita(nombre : str, id : str, fecha: str):
    
    data_finca = setle_clean(nombre)
    df_gps = filtro_finca(df_row,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    df_gps = df_gps.sort_values(by='UUID')
    df_gps = data_devices(df_gps,id)
    df_gps = df_gps.sort_values(by='createdAt')
    df_gps = select_data_by_date(df_gps,fecha)
    df_gps = dataframe_interview_vaca(df_gps)

    return JSONResponse(content= json.loads(df_gps.to_json()))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)