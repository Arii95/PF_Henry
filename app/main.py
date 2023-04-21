#Importo librerias
import matplotlib.pyplot as plt
import pandas as pd
import geopandas as gpd
import pymongo
import math
import json
import random
from sklearn.cluster import KMeans
from datetime import timedelta
from pymongo import MongoClient
from typing import List
from shapely.geometry import Point
from geopy.distance import great_circle
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pymongo.mongo_client import MongoClient


# Crear una conexión a una instancia de MongoDB en la dirección 'localhost:27017'.
data_mongo: MongoClient = pymongo.MongoClient("localhost",27017)

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

def select_data_by_dates(df: pd.DataFrame, fecha_init: str, fecha_fin : str) -> pd.DataFrame:
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
    fecha_deseada1 = pd.to_datetime(fecha_init).date()
    fecha_deseada2 = pd.to_datetime(fecha_fin).date()

    nuevo_df = df[(df['createdAt'].dt.date >= fecha_deseada1) & (df['createdAt'].dt.date <= fecha_deseada2)]

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
                data_var = ((data.iloc[i + 1]['dataRowData_gpsVel'] - data.iloc[i - 1]['dataRowData_gpsVel']))/900
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



def entrenamiendo():
    from datetime import datetime

    def generar_fechas_aleatorias(fecha_inicial, fecha_final):
        """
        Función auxiliar para generar fechas aleatorias entre dos fechas dadas.
        """
        # Convertir las fechas a objetos datetime
        dt_fecha_inicial = datetime.strptime(fecha_inicial, '%Y-%m-%d %H:%M:%S')
        dt_fecha_final = datetime.strptime(fecha_final, '%Y-%m-%d %H:%M:%S')
        
        # Generar una fecha aleatoria entre las dos fechas dadas
        tiempo_aleatorio = random.random()
        dt_fecha = dt_fecha_inicial + tiempo_aleatorio * (dt_fecha_final - dt_fecha_inicial)
        
        # Devolver la fecha como un string en el formato requerido
        return dt_fecha.strftime('%Y-%m-%d %H:%M:%S')

    def escribir_dataframe(num_lineas, nombre_columnas, rangos, valor_constante, posicion_constante, fecha_inicial, fecha_final, posicion_fecha_ini, posicion_fecha_next):
        # Crear una lista vacía para almacenar los datos generados
        datos = []
            
        # Generar los datos
        for i in range(num_lineas):
            # Generar una lista con valores aleatorios dentro de los rangos especificados
            fila = [random.uniform(rango[0], rango[1]) for rango in rangos]
            # Insertar el valor constante en la posición indicada por el argumento posicion_constante
            fila.insert(posicion_constante, valor_constante)
            # Generar fechas aleatorias entre las fechas dadas para la columna point_ini
            fecha_ini_aleatoria = generar_fechas_aleatorias(fecha_inicial, fecha_final)
            # Calcular la fecha para la columna point_next
            fecha_next_aleatoria = datetime.strptime(fecha_ini_aleatoria, '%Y-%m-%d %H:%M:%S') + timedelta(minutes=15)
            # Insertar las fechas aleatorias en las posiciones indicadas por los argumentos posicion_fecha_ini y posicion_fecha_next
            fila.insert(posicion_fecha_ini, fecha_ini_aleatoria)
            fila.insert(posicion_fecha_next, fecha_next_aleatoria.strftime('%Y-%m-%d %H:%M:%S'))
            datos.append(fila)
        
        # Crear un DataFrame con los datos generados y las columnas correspondientes
        df = pd.DataFrame(datos, columns=nombre_columnas)
        
        return df

    # Definir los parámetros para llamar a la función escribir_csv
    num_lineas = 10000
    nombre_columnas = ["UUID", "point_ini", "point_next", "interval_time","distancia", "velocidad", "tiempo", "charge_vel","actividad"]
    rangos = [(9999999, 9999999), (-1,2), (0.007,0.01), (0.019,0.09), (0.180,0.315), (-0.047000,0.047000)]
    valor_constante = "pastoreo"
    posicion_constante = 8
    fecha_inicial = '2023-01-01 00:00:00'
    fecha_final = '2023-03-01 00:00:00'
    posicion_fecha_ini = 1
    posicion_fecha_next = 2



    fecha_ini_aleatoria = generar_fechas_aleatorias(fecha_inicial, fecha_final)
    fecha_next_aleatoria = datetime.strptime(fecha_ini_aleatoria, '%Y-%m-%d %H:%M:%S') + timedelta(minutes=15)

    # Llamar a la función escribir_csv con los parámetros de ejemplo
    df1 = escribir_dataframe(num_lineas, nombre_columnas, rangos, valor_constante, posicion_constante, fecha_inicial, fecha_final, posicion_fecha_ini, posicion_fecha_next)


    # Definir los parámetros para llamar a la función escribir_csv
    num_lineas = 10000
    nombre_columnas = ["UUID", "point_ini", "point_next", "interval_time","distancia", "velocidad", "tiempo", "charge_vel","actividad"]
    rangos = [(9999999, 9999999), (-1, 2), (0.0, 0.005), (0.0, 0.01), (0.0,0.18), (0.0 ,  0.00002 )]
    valor_constante = "rumeo"
    posicion_constante = 8
    fecha_inicial = '2023-01-01 00:00:00'
    fecha_final = '2023-03-01 00:00:00'
    posicion_fecha_ini = 1
    posicion_fecha_next = 2
    


    fecha_ini_aleatoria = generar_fechas_aleatorias(fecha_inicial, fecha_final)
    fecha_next_aleatoria = datetime.strptime(fecha_ini_aleatoria, '%Y-%m-%d %H:%M:%S') + timedelta(minutes=15)

    # Llamar a la función escribir_csv con los parámetros de ejemplo
    df2 = escribir_dataframe(num_lineas, nombre_columnas, rangos, valor_constante, posicion_constante, fecha_inicial, fecha_final, posicion_fecha_ini, posicion_fecha_next)

    # Concatenar los DataFrames verticalmente
    concatenado = pd.concat([df1, df2], axis=0, ignore_index=True)
    cambio = {"pastoreo":0,"rumeo":1}
    concatenado.actividad = concatenado.actividad.map(cambio)


    # seleccionar las características relevantes para el clustering
    X = concatenado[['distancia','velocidad','charge_vel','tiempo']].values
    y = concatenado['actividad']

    # crear el modelo de K-means con 2 clusters
    kmeans = KMeans(n_clusters=2)

    # ajustar el modelo a los datos
    kmeans.fit(X,y)

    # obtener las etiquetas de cluster asignadas a cada fila
    labels = kmeans.labels_
    # añadir las etiquetas al dataframe
    concatenado['cluster'] = labels

    return kmeans


def grafico_cluster(data):
    counts = data['cluster'].value_counts()
    plt.bar(counts.index, counts.values)
    plt.xlabel('Cluster')
    plt.ylabel('Frecuencia')
    plt.title('Frecuencia de clusters')

    # mostrar el gráfico
    return plt.show()



def prediccion(df):
    Y = df[['distancia','velocidad','charge_vel','tiempo']].values

    perro = entrenamiendo().predict(Y)

    # añadir las etiquetas al dataframe
    df['cluster'] = perro
    return df

def add_dormida_column(df, cluster_val, start_time, end_time):
    df['dormida'] = 'NO'
    for i, row in df.iterrows():
        if row['cluster'] == cluster_val:
            hora = pd.to_datetime(row['point_ini']) - pd.Timedelta(hours=3)
            if hora.hour >= start_time or hora.hour < end_time:
                df.loc[i, 'dormida'] = 'SI'
    return df

def contar_actividades(df, cluster_rum, cluster_rum_2):
    # Crear las columnas "pastando", "rumeando" y "durmiendo" y establecer el valor inicial a 0
    df["pastando"] = 0
    df["rumeando"] = 0
    df["durmiendo"] = 0

    # Recorrer el DataFrame y contar las actividades según las condiciones dadas
    for i, row in df.iterrows():
        if row["dormida"] == "SI":
            df.at[i, "durmiendo"] += 1
        elif row["cluster"] == cluster_rum and row["dormida"] == "NO":
            df.at[i, "rumeando"] += 1
        elif row["cluster"] == cluster_rum_2:
            df.at[i, "pastando"] += 1

    # Crear un nuevo DataFrame con los valores totales de cada actividad
    total_df = pd.DataFrame({
        "pastando": [df["pastando"].sum()],
        "rumeando": [df["rumeando"].sum()],
        "durmiendo": [df["durmiendo"].sum()]
    })

    return total_df





#Creo una instancia de FastAPI
app = FastAPI()

#---- PRESENTACIÓN--------

@app.get("/")
def bienvenida():
    return "Bienvenidos a Bastó - Ganaderia Inteligente"

@app.get("/menu")
def menu():
    return "Las funciones que encontrara son las siguientes: (1) get_max_duration (2) get_score_count (3) get_count_platform (4) get_actor"


#Primer consulta: Toda la informacion de un establecimiento
@app.get("/filtro_por_establecimiento/{nombre}")
async def filtro_por_establecimiento(nombre : str):

    
    data_finca = setle_clean(nombre)
    prueba = filtro_finca(df_row,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    vacas= prueba.UUID.unique()
    data_nuevo={}
    for i in vacas:
        data=data_devices(prueba,i)
        data_nuevo[i]=dataframe_interview_vaca(data)
    merge_data= pd.concat(data_nuevo.values(),keys=data_nuevo.keys())
    merge_data.reset_index(level=0,inplace=True)
    merge_data.rename(columns={'level_0':'UUID'},inplace=True)
    merge_data.reset_index(inplace=True)
    merge_data.set_index("UUID")
    merge_data.drop(columns="index",inplace=True)

    return JSONResponse(content= json.loads(merge_data.to_json()))


#Cuarta Consulta: Toda la informacion de una vaca, en un establecimiento en un periodo de tiempo
@app.get("/informacion_por_un_periodo_por_finca/{nombre}/{fecha_init}/{fecha_fin}")
def informacion_por_un_periodo_por_finca(nombre : str, fecha_init: str, fecha_fin : str):
    
    data_finca = setle_clean(nombre)
    prueba = filtro_finca(df_row,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    prueba = prueba.sort_values(by='createdAt')
    prueba = select_data_by_dates(prueba,fecha_init,fecha_fin)
    vacas= prueba.UUID.unique()
    data_nuevo={}
    for i in vacas:
        data=data_devices(prueba,i)
        data_nuevo[i]=dataframe_interview_vaca(data)
    merge_data= pd.concat(data_nuevo.values(),keys=data_nuevo.keys())
    merge_data.reset_index(level=0,inplace=True)
    merge_data.rename(columns={'level_0':'UUID'},inplace=True)
    merge_data.reset_index(inplace=True)
    merge_data.set_index("UUID")
    merge_data.drop(columns="index",inplace=True)
    merge_data.point_ini= merge_data.point_ini.astype(str)
    merge_data.point_next= merge_data.point_next.astype(str)

    return JSONResponse(content= json.loads(merge_data.to_json()))

#Segunda consulta: Toda la informacion de una vaca de un establecimiento
@app.get("/filtro_por_una_vaca_establecimiento/{nombre}/{id}")
async def filtro_por_una_vaca_establecimiento(nombre : str, id : str):

    
    data_finca = setle_clean(nombre)
    prueba = filtro_finca(df_row,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    prueba = prueba.sort_values(by='UUID')
    prueba = data_devices(prueba,id)
    vacas= prueba.UUID.unique()
    data_nuevo={}
    for i in vacas:
        data=data_devices(prueba,i)
        data_nuevo[i]=dataframe_interview_vaca(data)
    merge_data= pd.concat(data_nuevo.values(),keys=data_nuevo.keys())
    merge_data.reset_index(level=0,inplace=True)
    merge_data.rename(columns={'level_0':'UUID'},inplace=True)
    merge_data.reset_index(inplace=True)
    merge_data.set_index("UUID")
    merge_data.drop(columns="index",inplace=True)

    return JSONResponse(content= json.loads(merge_data.to_json()))

#Tercer Consulta: Toda la informacion de una vaca, en un establecimiento en una fecha
@app.get("/informacion_por_un_dia_una_vaca_por_finca/{nombre}/{id}/{fecha}")
def informacion_por_un_dia_una_vaca_por_finca(nombre : str, id : str, fecha: str):
    
    data_finca = setle_clean(nombre)
    prueba = filtro_finca(df_row,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    prueba = prueba.sort_values(by='UUID')
    prueba = data_devices(prueba,id)
    prueba = prueba.sort_values(by='createdAt')
    prueba = select_data_by_date(prueba,fecha)
    vacas= prueba.UUID.unique()
    data_nuevo={}
    for i in vacas:
        data=data_devices(prueba,i)
        data_nuevo[i]=dataframe_interview_vaca(data)
    merge_data= pd.concat(data_nuevo.values(),keys=data_nuevo.keys())
    merge_data.reset_index(level=0,inplace=True)
    merge_data.rename(columns={'level_0':'UUID'},inplace=True)
    merge_data.reset_index(inplace=True)
    merge_data.set_index("UUID")
    merge_data.drop(columns="index",inplace=True)
    merge_data.point_ini= merge_data.point_ini.astype(str)
    merge_data.point_next= merge_data.point_next.astype(str)

    return JSONResponse(content= json.loads(merge_data.to_json()))


#Cuarta Consulta: Toda la informacion de una vaca, en un establecimiento en un periodo de tiempo
@app.get("/informacion_por_un_periodo_una_vaca_por_finca/{nombre}/{id}/{fecha_init}/{fecha_fin}")
def informacion_por_un_periodo_una_vaca_por_finca(nombre : str, id : str, fecha_init: str, fecha_fin : str):
    
    data_finca = setle_clean(nombre)
    prueba = filtro_finca(df_row,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    prueba = prueba.sort_values(by='UUID')
    prueba = data_devices(prueba,id)
    prueba = prueba.sort_values(by='createdAt')
    prueba = select_data_by_dates(prueba,fecha_init,fecha_fin)
    vacas= prueba.UUID.unique()
    data_nuevo={}
    for i in vacas:
        data=data_devices(prueba,i)
        data_nuevo[i]=dataframe_interview_vaca(data)
    merge_data= pd.concat(data_nuevo.values(),keys=data_nuevo.keys())
    merge_data.reset_index(level=0,inplace=True)
    merge_data.rename(columns={'level_0':'UUID'},inplace=True)
    merge_data.reset_index(inplace=True)
    merge_data.set_index("UUID")
    merge_data.drop(columns="index",inplace=True)
    merge_data.point_ini= merge_data.point_ini.astype(str)
    merge_data.point_next= merge_data.point_next.astype(str)

    return JSONResponse(content= json.loads(merge_data.to_json()))

@app.get("/conducta_vaca/{nombre}/{id}/{fecha}")
def conducta_vaca(nombre : str, id : str, fecha: str):
    data_finca = setle_clean(nombre)
    prueba = filtro_finca(df_row,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    prueba = prueba.sort_values(by='UUID')
    prueba = data_devices(prueba,id)
    prueba = prueba.sort_values(by='createdAt')
    prueba = select_data_by_date(prueba,fecha)
    vacas= prueba.UUID.unique()
    data_nuevo={}
    for i in vacas:
        data=data_devices(prueba,i)
        data_nuevo[i]=dataframe_interview_vaca(data)
    merge_data= pd.concat(data_nuevo.values(),keys=data_nuevo.keys())
    merge_data.reset_index(level=0,inplace=True)
    merge_data.rename(columns={'level_0':'UUID'},inplace=True)
    merge_data.reset_index(inplace=True)
    merge_data.set_index("UUID")
    merge_data.drop(columns="index",inplace=True)
    merge_data.point_ini= merge_data.point_ini.astype(str)
    merge_data.point_next= merge_data.point_next.astype(str)

    entrenamiendo()
    dormida_df = add_dormida_column(prediccion(merge_data), 1, 20, 7)
    resultados = contar_actividades(dormida_df, 1, 0)

    return JSONResponse(content= json.loads(resultados.to_json()))


@app.get("/conducta_vaca_periodo/{nombre}/{id}/{fecha_init}/{fecha_fin}")
def conducta_vaca_periodo(nombre : str, id : str, fecha_init: str, fecha_fin : str):
    
    data_finca = setle_clean(nombre)
    prueba = filtro_finca(df_row,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    prueba = prueba.sort_values(by='UUID')
    prueba = data_devices(prueba,id)
    prueba = prueba.sort_values(by='createdAt')
    prueba = select_data_by_dates(prueba,fecha_init,fecha_fin)
    vacas= prueba.UUID.unique()
    data_nuevo={}
    for i in vacas:
        data=data_devices(prueba,i)
        data_nuevo[i]=dataframe_interview_vaca(data)
    merge_data= pd.concat(data_nuevo.values(),keys=data_nuevo.keys())
    merge_data.reset_index(level=0,inplace=True)
    merge_data.rename(columns={'level_0':'UUID'},inplace=True)
    merge_data.reset_index(inplace=True)
    merge_data.set_index("UUID")
    merge_data.drop(columns="index",inplace=True)
    merge_data.point_ini= merge_data.point_ini.astype(str)
    merge_data.point_next= merge_data.point_next.astype(str)

    entrenamiendo()
    dormida_df = add_dormida_column(prediccion(merge_data), 1, 20, 7)
    resultados = contar_actividades(dormida_df, 1, 0)

    return JSONResponse(content= json.loads(resultados.to_json()))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

