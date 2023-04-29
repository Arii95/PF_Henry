import datetime
import pandas as pd
import pymongo
import numpy as np

data_mongo = pymongo.MongoClient('localhost:27017') #'mongodb+srv://brandon:brandon1@cluster0.tfvievv.mongodb.net/?retryWrites=true&w=majority')
# )#
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


def setle_list():
    setle_n= mongo_data('settlements')
    setle_n['latitud_c']=setle_n.centralPoint.apply(lambda x: x[0]['lat'] if 'lat' in x[0] else None)
    setle_n['longitud_c']=setle_n.centralPoint.apply(lambda x: x[0]['lng'] if 'lng' in x[0] else None)
    setle_n = setle_n[['_id','hectares','name','latitud_c','longitud_c']]
    mascara= setle_n._id.isin(['63ff75624c2d6d003084c117','642b1d27cc00091984864f0a','642c0b596490e600305e1819'])
    setle_n.drop(setle_n[mascara].index,inplace=True)
    return setle_n

def setle_clean(select):
    de= db['settlements']
    obj= de.find_one({'name':select})
    df_setle= pd.json_normalize(obj,sep='')
    df_setle['latitud_c']=df_setle.centralPoint.apply(lambda x: x[0]['lat'] if 'lat' in x[0] else None)
    df_setle['longitud_c']=df_setle.centralPoint.apply(lambda x: x[0]['lng'] if 'lng' in x[0] else None)
    setle_n = df_setle[['_id','hectares','registerNumber','headsCount','name','latitud_c','longitud_c']]
    return setle_n

def selec_setle(data,select):
    df_setle = data[data._id== select]
    return df_setle

def conect_animal():
        df_animal=mongo_data('animals')
        df_animal['animalSettlement']=df_animal['animalSettlement'].apply(lambda x:x[0])
        df_animal.animalSettlement=df_animal.animalSettlement.astype(str)
        return df_animal

def selec_anim(data,select):
    df_anim= data[data._id==select]
    return df_anim


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
        semana = datetime.datetime.strptime(semana, '%Y-%m-%d')
        
    dia_semana = semana.weekday()
    
    fecha_inicio = semana - datetime.timedelta(days=dia_semana)
    fecha_fin = fecha_inicio + datetime.timedelta(days=6)
    
    fecha_inicio = fecha_inicio.strftime('%Y-%m-%d')
    fecha_fin = fecha_fin.strftime('%Y-%m-%d')
    return fecha_inicio, fecha_fin



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