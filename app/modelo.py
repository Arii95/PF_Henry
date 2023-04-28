import pandas as pd
from geopy.distance import great_circle
from starlette.responses import RedirectResponse
from ml_suport import predict_model
from aguadas import agua_click,result_select,agua_clicks
import pandas as pd
import csv
import pandas as pd
import geopandas as gpd
import pymongo
from pymongo import MongoClient
from shapely.geometry import Point
import math
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

##  transform_data



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
    df = pd.DataFrame(df,columns=['point_ini','point_next' ,'interval_time','distancia','velocidad','tiempo']) 
    df['aceleracion']= df['velocidad'].diff()/df['tiempo'].diff()
    df['p_distancia']= df['velocidad'] * df['tiempo'] 
    return df


def data_interview(nombre,data=df_row):
    data_finca = setle_clean(nombre)
    prueba = filter_area_perimetro(data,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
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
    return merge_data



# DATOS RESUMIDOS----------------------------------------------------


def add_dormida_column(df, cluster_val, start_time, end_time):
    df['dormida'] = 'NO'
    for i, row in df.iterrows():
        if row['cluster'] == cluster_val:
            hora = pd.to_datetime(row['point_ini']) - pd.Timedelta(hours=3)
            if hora.hour >= start_time or hora.hour < end_time:
                df.loc[i, 'dormida'] = 'SI'
    return df


def cosa(numero_horas):
    horas = int(numero_horas)
    minutos = int((numero_horas - horas) * 60)
    segundos = int(((numero_horas - horas) * 60 - minutos) * 60)
    return f"{horas} h, {minutos} min, {segundos} seg"


def acumular_diferencia_tiempo(df, cluster_rum, cluster_rum_2):
    # Convertir las columnas "point_ini" y "point_next" en valores de tipo datetime
    df["point_ini"] = pd.to_datetime(df["point_ini"])
    df["point_next"] = pd.to_datetime(df["point_next"])

    # Crear las columnas "rumeando", "pastando" y "durmiendo" y establecer el valor inicial a 0
    df["rumeando"] = 0
    df["pastando"] = 0
    df["durmiendo"] = 0
    df["bebiendo"] = 0
    cantidadregistro=0

    # Recorrer el DataFrame y sumar los valores de la diferencia entre "point_ini" y "point_next" según las condiciones dadas
    for i, row in df.iterrows():
        if row["dormida"] == "SI" and row['agua'] == 0:
            df.at[i, "durmiendo"] += ((row["point_next"] - row["point_ini"]).total_seconds())/3600
        elif row["cluster"] == 1 and row["dormida"] == "NO" and row['agua'] == 0:
            df.at[i, "rumeando"] += ((row["point_next"] - row["point_ini"]).total_seconds())/3600
        elif row["cluster"] == 0 and row['agua'] == 0:
            df.at[i, "pastando"] += ((row["point_next"] - row["point_ini"]).total_seconds())/3600
        elif row['agua'] == 1 :
            df.at[i, "bebiendo"] += ((row["point_next"] - row["point_ini"]).total_seconds())/3600
        cantidadregistro +=1
    # Crear un nuevo DataFrame con los valores totales de cada actividad
    total_df = pd.DataFrame({
        "rumiando": [cosa(df["rumeando"].sum())],
        "pastando": [cosa(df["pastando"].sum())],
        "durmiendo": [cosa(df["durmiendo"].sum())],
        "bebiendo": [cosa(df["bebiendo"].sum())],
        "cant_registro": cantidadregistro
    })
    
    return total_df


def separador_por_dia(df):
    df['fecha']= pd.to_datetime(df.point_ini).dt.date
    
    diarios= {}
    for fecha,grupo in df.groupby(df['point_ini'].dt.date):
        diarios[fecha]=acumular_diferencia_tiempo(grupo,1,0)
    diarios=pd.concat(diarios.values(),keys=diarios.keys(),axis=0)
    diarios=diarios.reset_index(level=1).drop(columns=['level_1'])
    return diarios 

# DIAGNOSTICO -------------------------------------------------


def respuesta_diagnostico(valor,min,max):
    if valor > min and valor < (max+(max*0.05)):
        result='normal'
    elif valor > (min-(min*0.25)) and valor < (max+(max*0.25)):
        result= 'atencion!' 
    else:
        result= 'mal'
    return result


def diagnostico_devices(df):
    rumia=[float(x.split('h')[0]) for x in df['rumiando']]
    pastoreo=[float(x.split('h')[0]) for x in df['pastando']]
    durmiendo=[float(x.split('h')[0]) for x in df['durmiendo']]
    agua=[float(x.split('h')[0]) for x in df['bebiendo']]
    can_r=['optimo' if x >= 72 else 'poco' if x < 68 else 'no optimo' for x in df['cant_registro'] ]
    
    diag= pd.DataFrame({
        'fecha':[x for x in df.index],
        'rumiando':[respuesta_diagnostico(x,6,8) for x in rumia] ,
        'pastando':[respuesta_diagnostico(x,8,12) for x in pastoreo],
        'durmiendo':[respuesta_diagnostico(x,5,8) for x in durmiendo],
        'agua':[respuesta_diagnostico(x,1,4) for x in agua] ,
        'cant_registro':can_r,
    })
    return diag



## Support_api


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


# def agregar_ith(data, fecha,asentamiento_id):
#     df_setith= mongo_data('settlementithcounts')
#     df_setith.settlementId =df_setith.settlementId.astype(str)
#     pru= df_setith[df_setith.settlementId ==asentamiento_id]
#     prueba=pru[pru.createdAt.dt.date ==pd.to_datetime(fecha)]
#     if prueba.shape[0]!=0:
#         prueba_ith= pd.merge(data,prueba[['createdAt', 'ITH']],left_on=data.point_ini.dt.hour, right_on=prueba['createdAt'].dt.hour)
#         prueba_ith= prueba_ith.drop(columns=['key_0','createdAt'])
#         return prueba_ith
#     else:
#         return f'fecha vacia'


def agregar_ith(data, fecha_ini,asentamiento_id,fecha_fin=None):
    df_setith= mongo_data('settlementithcounts')
    df_setith.settlementId =df_setith.settlementId.astype(str)
    pru= df_setith[df_setith.settlementId ==asentamiento_id]
    if fecha_fin== None:
        prueba=pru[pru.createdAt.dt.date ==pd.to_datetime(fecha_ini)]
        if prueba.shape[0]!=0:
            prueba_ith= pd.merge(data,prueba[['createdAt', 'ITH']],left_on=data.point_ini.dt.hour, right_on=prueba['createdAt'].dt.hour)
            prueba_ith= prueba_ith.drop(columns=['key_0','createdAt'])
            return prueba_ith
    else:
        prueba=pru[(pru.createdAt.dt.date >= pd.to_datetime(fecha_ini)) & (pru.createdAt.dt.date <= pd.to_datetime(fecha_fin))]
        if prueba.shape[0]!=0:
            prueba_ith= pd.merge(data,prueba[['createdAt', 'ITH']],left_on=data.point_ini.dt.hour, right_on=prueba['createdAt'].dt.hour)
            prueba_ith= prueba_ith.drop(columns=['key_0','createdAt'])
            return prueba_ith
        

## ML_Support


# Dataframe para pastoreo
def dataframe_entrenamiento():
    pastoreo_df = pd.DataFrame({
        'distancia': np.random.normal(loc=0.025, scale=0.01, size=7000),
        'velocidad': np.random.normal(loc=0.2, scale=0.05, size=7000),
        'tiempo': np.random.normal(loc=0.15, scale=0.05, size=7000),
        'aceleracion': np.random.normal(loc=-0.2, scale=0.1, size=7000),
        'actividad': 'pastoreo'
    })

    # Dataframe para rumia
    rumia_df = pd.DataFrame({
        'distancia': np.random.normal(loc=0.005, scale=0.002, size=7000),
        'velocidad': np.random.normal(loc=0.01, scale=0.002, size=7000),
        'tiempo': np.random.normal(loc=0.5, scale=0.05, size=7000),
        'aceleracion': np.random.normal(loc=-0.05, scale=0.02, size=7000),
        'actividad': 'rumia'
    })
    #Concatenado y mezclado de ambos dataframe para entrenado
    concatenado = pd.concat([pastoreo_df, rumia_df], axis=0, ignore_index=True)
    concatenado= concatenado.sample(frac=1,random_state=42).reset_index(drop=True)
    cambio={'pastoreo':0,'rumia':1}
    concatenado.actividad= concatenado.actividad.map(cambio)
    return concatenado


def fit_model():
    concatenado = dataframe_entrenamiento()
    scaler= StandardScaler()
    data_sca= scaler.fit_transform(concatenado[['velocidad',  'aceleracion']])
    y=concatenado['actividad']
    kmeans= KMeans(n_clusters=2 , random_state=42)
    kmeans.fit(data_sca,y)
    return kmeans


def predict_model(data):
    kmeans =fit_model()
    data = data.fillna(0.0)
    data.loc[(data.aceleracion == np.inf) | (data.aceleracion == -np.inf),'aceleracion']=0.0
    x_test = data[['velocidad','aceleracion']].values#'p_distancia',
    perro = kmeans.predict(x_test)
    data['cluster'] = perro
    return data



## 

def conducta_vaca(nombre : str, id : str, fecha: str):
    data_finca = setle_clean(nombre)
    finca = filter_area_perimetro(df_gps,data_finca['latitud_c'],data_finca['longitud_c'],data_finca['hectares'])
    df_gp= data_devices(finca,id)
    df_gp = select_data_by_date(df_gp,fecha)
    df_gp = dataframe_interview_vaca(df_gp)
    df_gp = predict_model(df_gp)
    d = agua_click(finca, id ,fecha ,str(data_finca._id.values[0]))
    df_gp =result_select(df_gp,d)
    df_gp = add_dormida_column(df_gp, 1, 20, 6)
    resumen=separador_por_dia(df_gp)
    resumen.index= resumen.index.astype(str)
    df_gp.point_ini= df_gp.point_ini.astype(str)
    df_gp.point_next= df_gp.point_next.astype(str)
    diagnostico = diagnostico_devices(resumen)
    df_gp=df_gp.drop(columns=['fecha'])
    datos={'datos':df_gp.to_dict('records'),'resumen_datos':resumen.to_dict('records'),'diagnostico':diagnostico.to_dict('records')}
    return datos
            

conducta_vaca('MACSA', '0004A30B00F89C52', '2023-03-30')