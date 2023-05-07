import pandas as pd
import geopandas as gpd
import pymongo
import math
from shapely.geometry import Point
from pymongo.collection import Collection

# Crear una conexión a una instancia de MongoDB
data_mongo = pymongo.MongoClient("localhost:27017")

# Seleccionar una base de datos existente o crear una nueva llamada 'test'.
db = data_mongo["test"]

# Seleccionar una colección de la base de datos llamada 'datarows'.
rows = db["datarows"]
data_row = rows.find({"dataRowType": "GPS"})
df_row = pd.json_normalize(data_row, sep="_")
df_row._id = df_row._id.astype(str)

df_gps = df_row[
    [
        "UUID",
        "dataRowType",
        "createdAt",
        "updatedAt",
        "dataRowData_lat",
        "dataRowData_lng",
        "dataRowData_gpsAlt",
        "dataRowData_gpsVel",
        "dataRowData_gpsFixed",
    ]
]


def mongo_data(collection: str) -> pd.DataFrame:
    """
    Obtiene datos de una colección de MongoDB y los devuelve como un DataFrame.

    Parámetros:
    -----------
    - collection (str): Nombre de la colección a consultar.
    - db (Database): Instancia de la base de datos en la que se encuentra la colección.

    Retorna:
    -----------
    - df (pd.DataFrame): Un DataFrame con los datos de la colección especificada.
    """
    mongoColle: Collection = db[collection]
    data = list(mongoColle.find())
    df: pd.DataFrame = pd.json_normalize(data, sep="_")
    df._id = df._id.astype(str)
    return df


def conect_animal() -> pd.DataFrame:
    """
    Obtiene los datos de los animales que se encuentran en las aguadas y puntos fijos.

    Retorna:
    -----------
    - result (pd.DataFrame): un DataFrame con los datos de los animales que se encuentran en las aguadas y puntos fijos.
    """
    df_animal: pd.DataFrame = mongo_data("animals")
    df_animal["animalSettlement"] = df_animal["animalSettlement"].apply(lambda x: x[0])
    df_animal.animalSettlement = df_animal.animalSettlement.astype(str)
    result: pd.DataFrame = df_animal[(df_animal.caravanaNumber.str.contains("AGUADA")) | (df_animal.caravanaNumber.str.contains("PUNTO_FIJO"))]
    return result


def update_aguada(setle: str) -> pd.DataFrame:
    """
    Obtiene los datos de las aguadas de un asentamiento en específico.

    Parámetros:
    -----------
    - setle (str): Cadena de caracteres que indica el nombre del asentamiento.

    Retorna:
    -----------
    - agua (pd.DataFrame): un DataFrame con los datos de las aguadas de un asentamiento en específico.
    """
    
    df_devis= mongo_data('devices')
    df_devis.deviceAnimalID=df_devis.deviceAnimalID.astype(str)
    data_devise = df_devis[df_devis.deviceType=='PUNTO FIJO'] 
    aguadas= conect_animal()
    aguadas.animalSettlement = aguadas.animalSettlement.apply(lambda x:str(x)) 
    x= aguadas[aguadas.animalSettlement == str(setle).strip()]
    agua =data_devise[data_devise.deviceAnimalID.isin(x._id.values)]

    return agua


def setle_clean(select: str) -> pd.DataFrame:
    """
    Limpia los datos de una colección de MongoDB que contiene información de asentamientos.

    Parámetros:
    -----------
    - select (str): El nombre del asentamiento que se desea limpiar.

    Retorna:
    -----------
    - setle_n (pd.DataFrame): Un objeto de tipo DataFrame de Pandas con los datos limpios.
    """
    de= db['settlements']
    obj= de.find({'name':select})
    df_setle= pd.json_normalize(obj)
    df_setle._id = df_setle._id.astype(str)
    df_setle['latitud_c']=df_setle.centralPoint.apply(lambda x: x[0]['lat'] if 'lat' in x[0] else None)
    df_setle['longitud_c']=df_setle.centralPoint.apply(lambda x: x[0]['lng'] if 'lng' in x[0] else None)
    setle_n = df_setle[['_id','hectares','registerNumber','headsCount','name','latitud_c','longitud_c']]
    return setle_n

def filter_devices(data: pd.DataFrame, uuid: str) -> pd.DataFrame:
    """
    Filtra los datos de un DataFrame que corresponden a un dispositivo específico
    y elimina las filas con valores faltantes en la columna dataRowData_lat.

    Parámetros:
    -----------
    - data (pd.DataFrame): DataFrame que contiene los datos a filtrar.
    - uuid (str): string que corresponde al identificador único del dispositivo a filtrar.

    Retorna:
    --------
    - data (pd.DataFrame): un DataFrame que contiene solo los datos del dispositivo especificado, sin valores faltantes en dataRowData_lat.
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
    - hectarea (float): área del terreno en hectáreas

    Retorna:
    -----------
    - perim (float): perímetro aproximado del terreno en metros
    """
    hect = hectarea  # Asignamos el valor del parámetro hectarea a la variable hect
    lado = (
        math.sqrt(hect) * 10
    )  # Calculamos la longitud del lado de un cuadrado cuya área es igual a hect y multiplicamos por 10
    perim = (
        lado * 4
    )  # Calculamos el perímetro del cuadrado multiplicando la longitud del lado por 4

    return perim


def filter_area_perimetro(data:pd.DataFrame,setle:str):
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


def select_data_by_date(df: pd.DataFrame, fecha: str) -> pd.DataFrame:
    """
    Selecciona las filas de un DataFrame correspondientes a una fecha específica.

    Parámetros:
    -----------
    - df (pd.DataFrame): DataFrame de pandas que contiene la columna "createdAt".
    - fecha (str): Fecha en formato de cadena, en el formato 'YYYY-MM-DD'.

    Returno:
    -----------
    - nuevo_df (pd.DataFrame): DataFrame de pandas que contiene solo las filas correspondientes a la fecha especificada.
    """

    # Verificar que el DataFrame tenga la columna 'createdAt'
    if "createdAt" not in df.columns:
        raise ValueError("El DataFrame no contiene la columna 'createdAt'")

    # Verificar que la variable "fecha" esté en el formato correcto
    try:
        fecha_deseada = pd.to_datetime(fecha)
    except ValueError:
        raise ValueError("La variable 'fecha' debe estar en el formato 'YYYY-MM-DD'")

    # Seleccionar solo las filas correspondientes a la fecha especificada
    nuevo_df = df.loc[df["createdAt"].dt.date == fecha_deseada.date()]

    return nuevo_df


def select_data_by_dates(df: pd.DataFrame, fecha_init: str, fecha_fin: str) -> pd.DataFrame:
    """
    Selecciona las filas de un DataFrame correspondientes a una fecha específica.

    Parametros:
    -----------
    - df (pd.DataFrame): DataFrame de pandas que contiene la columna "createdAt".
    - fecha_init (str): Fecha de inicio, en formato de cadena, en el formato 'YYYY-MM-DD'.
    - fecha_fin (str): Fecha final, en formato de cadena, en el formato 'YYYY-MM-DD'.

    Returno:
    -----------
    - nuevo_df (pd.DataFrame): DataFrame de pandas que contiene solo las filas correspondientes a la fecha especificada.
    """

    # Convertir la columna "createdAt" en un objeto datetime
    df["createdAt"] = pd.to_datetime(df["createdAt"])

    # Seleccionar solo las filas correspondientes a la fecha especificada
    fecha_deseada1 = pd.to_datetime(fecha_init).date()
    fecha_deseada2 = pd.to_datetime(fecha_fin).date()

    nuevo_df = df[
        (df["createdAt"].dt.date >= fecha_deseada1)
        & (df["createdAt"].dt.date <= fecha_deseada2)
    ]

    return nuevo_df

#




