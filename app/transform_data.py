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
    data = data.reset_index(drop=True)
    data_shifted = data.shift(1)
    data_shifted.columns = [f'{col}_next' for col in data_shifted.columns]
    merged_data = pd.concat([data, data_shifted], axis=1)

    merged_data = merged_data.iloc[1:-1]
    merged_data['distancia'] = merged_data.apply(lambda row: great_circle((row['dataRowData_lat'], row['dataRowData_lng']), (row['dataRowData_lat_next'], row['dataRowData_lng_next'])).kilometers, axis=1)
    merged_data['interval_time'] = (merged_data['createdAt_next'] - merged_data['createdAt']).astype('timedelta64[h]')

    merged_data['velocidad'] = round(merged_data['dataRowData_gpsVel'], 3)
    merged_data.loc[merged_data['velocidad'].isna(), 'velocidad'] = round(merged_data['distancia'] / merged_data['interval_time'] / pd.Series(merged_data['velocidad'].dropna()).mean(), 3)

    merged_data['tiempo'] = round(merged_data['distancia'] / merged_data['velocidad'], 3)

    df = merged_data[['createdAt', 'createdAt_next', 'interval_time', 'distancia', 'velocidad', 'tiempo']]
    df.columns = ['point_ini', 'point_next', 'interval_time', 'distancia', 'velocidad', 'tiempo']
    df['aceleracion'] = df['velocidad'].diff() / df['tiempo'].diff()
    df['p_distancia'] = df['velocidad'] * df['tiempo']
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

    # Recorrer el DataFrame y sumar los valores de la diferencia entre "point_ini" y "point_next" según las condiciones dadas
    for i, row in df.iterrows():
        if row["dormida"] == "SI" and row['agua'] == 0:
            df.at[i, "durmiendo"] += ((row["point_next"] - row["point_ini"]).total_seconds())/3600
        elif row["cluster"] == cluster_rum and row["dormida"] == "NO" and row['agua'] == 0:
            df.at[i, "rumeando"] += ((row["point_next"] - row["point_ini"]).total_seconds())/3600
        elif row["cluster"] == cluster_rum_2 and row['agua'] == 0:
            df.at[i, "pastando"] += ((row["point_next"] - row["point_ini"]).total_seconds())/3600
        elif row['agua'] == 1 :
            df.at[i, "bebiendo"] += ((row["point_next"] - row["point_ini"]).total_seconds())/3600

    # Crear un nuevo DataFrame con los valores totales de cada actividad
    total_df = pd.DataFrame({
        "rumeando": [cosa(df["rumeando"].sum())],
        "pastando": [cosa(df["pastando"].sum())],
        "durmiendo": [cosa(df["durmiendo"].sum())],
        "bebiendo": [cosa(df["bebiendo"].sum())]
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