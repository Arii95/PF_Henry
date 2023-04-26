import pandas as pd
from geopy.distance import great_circle

from support_api import filter_area_perimetro,setle_clean,data_devices,df_row


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