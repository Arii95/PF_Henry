from geopy.distance import great_circle
from geopy import Point
import geopandas as gpd
from shapely.geometry import Point
import math
import pandas as pd
from datetime import datetime, timedelta
from conect_datarows import mongo_data,setle_clean

def distancia_recorrida(data):
    """funcion que puede arrojar la distancia[0], velocidad promedio[1], timepo que llevo el recorrido [2] recorrida entre el primer punto de la lista y el ultimo los datos tiene que ser solo gps.. hya otra funcion que la complemeta para limpiar estos datos
    Args:
        data (_type_): _description_
    Returns:
        _type_: _description_
    """
    cordena1=tuple(data.iloc[0][['dataRowData_lat','dataRowData_lng']].values)
    cordena2= tuple(data.iloc[-1][['dataRowData_lat','dataRowData_lng']].values)
    dista_km= great_circle(cordena1,cordena2).kilometers
    return dista_km


def perimetro_aprox(hectarea):
    """Funcion: funcion que saca el numero(float) del radio de un perimetra en kilometros
    Returns:
        _type_: float valor en kilometros del radio de un perimetro
    """
    hect=hectarea
    lado = math.sqrt(hect)*10
    perim = lado*4
    return perim


def data_devices(data:pd.DataFrame,uuid):
    data=data[data.UUID == uuid]
    data= data.sort_values('createdAt',ascending=True)
    return data

def gps_data(data):
    gps= data[['dataRowData_lat','dataRowData_lng']]
    gps = gps.dropna()
    return gps


def obtener_fecha_inicio_fin(semana):
    """
    Función que recibe una semana en formato de fecha y devuelve la fecha de inicio y finalización de esa semana.
    
    Args:
    semana (str o datetime): Semana en formato de fecha. Debe estar en formato 'YYYY-MM-DD'.
    
    Returns:
    fecha_inicio (str): Fecha de inicio de la semana en formato 'YYYY-MM-DD'.
    fecha_fin (str): Fecha de finalización de la semana en formato 'YYYY-MM-DD'.
    """
    
    if isinstance(semana, str):
        semana = datetime.strptime(semana, '%Y-%m-%d')
        
    dia_semana = semana.weekday()
    
    fecha_inicio = semana - timedelta(days=dia_semana)
    fecha_fin = fecha_inicio + timedelta(days=6)
    
    fecha_inicio = fecha_inicio.strftime('%Y-%m-%d')
    fecha_fin = fecha_fin.strftime('%Y-%m-%d')
    return fecha_inicio, fecha_fin


def get_range_week(year,week):
    enero_1= datetime(year,1,1)
    dia_para= (week-1)*7
    semana_ini= enero_1 + timedelta(days=dia_para)
    ultimo_dia_semana = semana_ini + timedelta(days=6)
    return semana_ini.strftime("%Y-%m-%d"),ultimo_dia_semana.strftime("%Y-%m-%d")

def count_day_hour(data):
    sep_time=data.groupby([data.createdAt.dt.day_name(),data.createdAt.dt.hour]).agg({'UUID':'count'}).rename(columns={'UUID':'count_register'}).reset_index(level=[1]).rename(columns={'createdAt':'hours'}).reset_index().rename(columns={'createdAt':'day'})
    daytime={'Friday':5,
            'Monday':1,
            'Saturday':6,
            'Sunday':7,
            'Thursday':4,
            'Tuesday':2,
            'Wednesday':3}
    sep_time.day= sep_time.day.map(daytime)
    return sep_time


def conect_animal():
        df_animal=mongo_data('animals')
        df_animal['animalSettlement']=df_animal['animalSettlement'].apply(lambda x:str(x[0]))
        result= df_animal[(df_animal.caravanaNumber.str.contains('AGUADA'))|(df_animal.caravanaNumber.str.contains('PUNTO_FIJO'))]
        return result

def update_aguada(setle):
        df_devis= mongo_data('devices')
        df_devis.deviceAnimalID=df_devis.deviceAnimalID.astype(str)
        data_devise = df_devis[df_devis.deviceType=='PUNTO FIJO'] 
        aguadas= conect_animal()
        aguadas.animalSettlement = aguadas.animalSettlement.apply(lambda x:str(x))  
        x= aguadas[aguadas.animalSettlement == setle]
        agua =data_devise[data_devise.deviceAnimalID.isin(x._id.values)]
        return agua


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
    fecha_deseada = pd.to_datetime(fecha)
    nuevo_df = df.loc[df['createdAt'].dt.date == fecha_deseada.date()]

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

def filter_area_perimetro(data:pd.DataFrame,setle:str):
    """Funcion que genera a partir de otro dataframe, un dataframe nuevo a partir de un un punto latitud longitud y la cantidad de hectareas fitra ese perimetro

    Args:
        data(dataframe): dataframe a filtrar
        latitud (gps): latitud de punto central
        longitud (gps): longitud de punto central
        hectareas (float): numero de hectareas que posee el terreno
    Returns:
        _type_: dataframe filtrado dentro de un perimetro generado
    """

    setle= setle_clean(setle)
    gdf= gpd.GeoDataFrame(data,crs='EPSG:4326',geometry=gpd.points_from_xy(data.dataRowData_lng,data.dataRowData_lat))
    setle_lat=setle['latitud_c'].values[0]
    setle_lng=setle['longitud_c'].values[0]
    hectareas=setle['hectares'].values[0]
    punto_referencia= Point(setle_lng,setle_lat)	
    per_kilo= perimetro_aprox(hectareas)
    circulo= punto_referencia.buffer(per_kilo/111.32) # valor 1 grado aprox en kilometro en el ecuador 
    on_perimetro= gdf[gdf.geometry.within(circulo)]
    agua = update_aguada(setle._id.values[0])
    on_perimetro = on_perimetro.drop(on_perimetro[on_perimetro['UUID'].isin(agua.deviceMACAddress.unique())].index)
    return on_perimetro

def dataframe_interview_vaca(data): # tratar de filtrar por perimetro porque si hay valores (que los hay)fuera de rango de los -90 90 da error
    data_dis=[]
    data_vel=[]
    data_time=[]
    data_inter= []
    data_in=[]
    data_fin=[]
    for i in range(0,data.shape[0]+1):
        try:
            dista_km= great_circle(tuple(data.iloc[i][['dataRowData_lat','dataRowData_lng']].values),tuple(data.iloc[i+1][['dataRowData_lat','dataRowData_lng']].values)).kilometers
            data_in.append(data.iloc[i][['createdAt']].values[0])
            data_fin.append(data.iloc[i+1][['createdAt']].values[0])
            interval= int(data.iloc[i+1][['createdAt']].values[0].strftime('%H')) - int(data.iloc[i][['createdAt']].values[0].strftime('%H'))
            data_inter.append(interval)
            if dista_km <= 8.:
                data_dis.append(round(dista_km,3))
            if data.iloc[i].dataRowData_gpsVel:
                data_vel.append(round(data.iloc[i].dataRowData_gpsVel,3))
                data_time.append(round(dista_km/data.iloc[i].dataRowData_gpsVel,3))
            else:
                data_time.append(round(dista_km/pd.Series(data_vel).mean().round(3),3))# les puede dar error si el array de velocidad esta vacio... toma el valor promedio de las velocidades que hay hasta el momento
        except IndexError:
            pass
    df = list(zip(data_in,data_fin,data_inter,data_dis,data_vel,data_time))
    df = pd.DataFrame(df,columns=['point_ini','point_next','interval_time','distancia','velocidad','tiempo']) 
    df['aceleracion']= df['velocidad'].diff()/df['tiempo'].diff()
    df['p_distancia']= df['velocidad'] * df['tiempo'] 
    df = df.fillna(0.0)
    return df


def transform(x):
    horas =int(x.replace(',','').split('h')[0])
    minutos=int(x.replace(',','').strip().split('h')[1].split('min')[0])
    if minutos > 50:
        horas +=1
    return horas