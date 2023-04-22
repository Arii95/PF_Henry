import pandas as pd
import geopandas as gpd
import pymongo
from pymongo import MongoClient
from shapely.geometry import Point
import math


# Crear una conexión a una instancia de MongoDB 
data_mongo: MongoClient = pymongo.MongoClient('localhost:27017')#'mongodb+srv://brandon:brandon1@cluster0.tfvievv.mongodb.net/?retryWrites=true&w=majority')

# Seleccionar una base de datos existente o crear una nueva llamada 'test'.
db = data_mongo['test']

# Seleccionar una colección de la base de datos llamada 'datarows'.
rows = db['datarows']
data_row= rows.find({'dataRowType':'GPS'})
df_row=pd.json_normalize(data_row, sep='_')
df_row._id = df_row._id.astype(str)

df_gps=df_row[['UUID','dataRowType','createdAt','updatedAt','dataRowData_lat','dataRowData_lng','dataRowData_gpsAlt','dataRowData_gpsVel','dataRowData_gpsFixed']]

def mongo_data(collection):
    mongoColle= db[collection]
    data= list(mongoColle.find())
    df= pd.json_normalize(data,sep='_')
    df._id=df._id.astype(str)
    return df

def conect_animal():
        df_animal=mongo_data('animals')
        df_animal['animalSettlement']=df_animal['animalSettlement'].apply(lambda x:x[0])
        df_animal.animalSettlement=df_animal.animalSettlement.astype(str)
        result= df_animal[(df_animal.caravanaNumber.str.contains('AGUADA'))|(df_animal.caravanaNumber.str.contains('PUNTO_FIJO'))]#lo use para extraer un csv con aguadas y puntos fijos
        return result


def update_aguada(setle):
        df_devis= mongo_data('devices')
        df_devis.deviceAnimalID=df_devis.deviceAnimalID.astype(str)
        data_devise = df_devis[df_devis.deviceType=='PUNTO FIJO'] 
        aguadas= conect_animal()
        x= aguadas[aguadas['animalSettlement']==setle]
        agua =data_devise[data_devise.deviceAnimalID.isin(x._id)]
        return agua
    
    
    

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


def filter_area_perimetro(data,latitud,longitud,hectareas):
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
    gdf= gpd.GeoDataFrame(data,crs='EPSG:4326',geometry=gpd.points_from_xy(data.dataRowData_lng,data.dataRowData_lat))
    setle_lat=latitud
    setle_lng=longitud
    punto_referencia= Point(setle_lng,setle_lat)	
    per_kilo= perimetro_aprox(hectareas)
    circulo= punto_referencia.buffer(per_kilo/111.32) # valor 1 grado aprox en kilometro en el ecuador 
    on_perimetro= gdf[gdf.geometry.within(circulo)]
    agua = update_aguada(on_perimetro)
    on_perimetro = on_perimetro.drop(on_perimetro[on_perimetro['UUID'].isin(agua.deviceMACAddress.unique())].index)
    return on_perimetro


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


def setle_clean(select):
    de= db['settlements']
    obj= de.find_one({'name':select})
    df_setle= pd.json_normalize(obj,sep='')
    df_setle['latitud_c']=df_setle.centralPoint.apply(lambda x: x[0]['lat'] if 'lat' in x[0] else None)
    df_setle['longitud_c']=df_setle.centralPoint.apply(lambda x: x[0]['lng'] if 'lng' in x[0] else None)
    setle_n = df_setle[['_id','hectares','registerNumber','headsCount','name','latitud_c','longitud_c']]
    return setle_n





