import pandas as pd
from geopy.distance import great_circle
from typing import Dict, Any, Optional, Union
from filter_and_conection import filter_area_perimetro, setle_clean, filter_devices,mongo_data,filter_devices, select_data_by_dates,select_data_by_date,df_gps
from ml_suport import predict_model
from aguadas import agua_clicks,result_select,agua_click



def dataframe_interview_vaca(data: pd.DataFrame) -> pd.DataFrame:
    """
    Función que procesa un DataFrame de datos de GPS para calcular la distancia recorrida, la velocidad promedio y el tiempo
    de recorrido entre cada par de puntos consecutivos. Además, agrega una columna con la relación de velocidad entre puntos
    consecutivos.

    Parámetros:
    -----------
    - data (pd.DataFrame): DataFrame de datos de GPS con columnas 'createdAt', 'dataRowData_lat', 'dataRowData_lng' y 'dataRowData_gpsVel'

    Retorna:
    -----------
    - df (pd.DataFrame): DataFrame con las columnas 'point_ini', 'point_next', 'interval_time', 'distancia', 'velocidad', 'tiempo' y 'charge_vel'
    """
    data_dis = []
    data_vel = []
    data_time = []
    data_inter = []
    data_in = []
    data_fin = []
    for i in range(0, data.shape[0] + 1):
        try:
            dista_km = great_circle(
                tuple(data.iloc[i][["dataRowData_lat", "dataRowData_lng"]].values),
                tuple(data.iloc[i + 1][["dataRowData_lat", "dataRowData_lng"]].values),
            ).kilometers
            data_in.append(data.iloc[i][["createdAt"]].values[0])
            data_fin.append(data.iloc[i + 1][["createdAt"]].values[0])
            interval = int(
                data.iloc[i + 1][["createdAt"]].values[0].strftime("%H")
            ) - int(data.iloc[i][["createdAt"]].values[0].strftime("%H"))
            data_inter.append(interval)
            if dista_km <= 8.0:
                data_dis.append(round(dista_km, 3))
            if data.iloc[i].dataRowData_gpsVel:
                data_vel.append(round(data.iloc[i].dataRowData_gpsVel, 3))
                data_time.append(round(dista_km / data.iloc[i].dataRowData_gpsVel, 3))
            else:
                data_time.append(
                    round(dista_km / pd.Series(data_vel).mean().round(3), 3)
                )  # les puede dar error si el array de velocidad esta vacio... toma el valor promedio de las velocidades que hay hasta el momento
        except IndexError:
            pass
    df = list(zip(data_in, data_fin, data_inter, data_dis, data_vel, data_time))
    df = pd.DataFrame(
        df,
        columns=[
            "point_ini",
            "point_next",
            "interval_time",
            "distancia",
            "velocidad",
            "tiempo",
        ],
    )
    df["aceleracion"] = df["velocidad"].diff() / df["tiempo"].diff()
    df["p_distancia"] = df["velocidad"] * df["tiempo"]
    return df


def data_interview(nombre: str, data: Optional[pd.DataFrame] = df_gps) -> pd.DataFrame:
    """
    Esta función toma un nombre de finca y un DataFrame opcional y devuelve un DataFrame
    con datos de entrevistas de vacas en esa finca. Si no se proporciona un DataFrame,
    utiliza un DataFrame predeterminado llamado "df_row".

    Parámetros:
    -----------
    - nombre (str): El nombre de la finca.
    - data (pandas.core.frame.DataFrame, opcional): El DataFrame que contiene los datos
        de la entrevista. Por defecto es None.

    Retorna:
    -----------
    - merge_data (pd.DataFrame): Un DataFrame con datos de entrevistas de vacas en la finca
        especificada por "nombre".

    """
    # Filtra las áreas y perímetros que se encuentran dentro de la finca.
    prueba = filter_area_perimetro(data,nombre)

    # Obtiene los identificadores únicos de vacas dentro de la finca.
    vacas = prueba.UUID.unique()

    # Crea un diccionario vacío para almacenar los datos de entrevista de cada vaca.
    data_nuevo: Dict[str, Any] = {}

    # Itera sobre los identificadores de vacas y obtiene sus datos de entrevista.
    for i in vacas:
        data = filter_devices(prueba, i)
        data_nuevo[i] = dataframe_interview_vaca(data)

    # Concatena los DataFrames de todas las vacas en uno solo.
    merge_data = pd.concat(data_nuevo.values(), keys=data_nuevo.keys())

    # Restablece el índice del DataFrame resultante.
    merge_data.reset_index(level=0, inplace=True)
    merge_data.rename(columns={"level_0": "UUID"}, inplace=True)
    merge_data.reset_index(inplace=True)
    merge_data.set_index("UUID")
    merge_data.drop(columns="index", inplace=True)

    # Devuelve el DataFrame resultante.
    return merge_data


def agregar_iths(data: pd.DataFrame, asentamiento_name: str) -> pd.DataFrame:
    
#0004A30B00F89C52
    try:
        setle= setle_clean(asentamiento_name)

        df_setith= mongo_data('settlementithcounts')
        df_setith.settlementId =df_setith.settlementId.astype(str)
        pru= df_setith[df_setith.settlementId == setle._id.values[0]]
        aux= {}
        for fecha in data.point_ini.dt.date.unique():
            prueba=pru[pru.createdAt.dt.date== fecha]
            dataq=data[data.point_ini.dt.date  == fecha]
            aux[fecha]= pd.merge(dataq,prueba[['createdAt', 'ITH']],left_on=dataq.point_ini.dt.hour, right_on=prueba['createdAt'].dt.hour)
        prueb= pd.concat(aux.values())  
        prueb= prueb.drop(columns=['key_0','createdAt'])  
        if prueb.shape[0] == 0: return data 
        return prueb
    except AttributeError:
        return data

# DATOS RESUMIDOS----------------------------------------------------

def add_dormida_column(df: pd.DataFrame, cluster_val: Any, start_time: int, end_time: int) -> pd.DataFrame:
    """
    Esta función agrega una nueva columna llamada 'dormida' a un DataFrame y le asigna
    un valor 'NO' por defecto. Luego, para cada fila en el DataFrame, si el valor en la
    columna 'cluster' coincide con el valor 'cluster_val' dado y la hora en la columna
    'point_ini' está dentro del rango de tiempo dado, la función cambia el valor de la
    columna 'dormida' a 'SI'.

    Parámetros:
    -----------
    - df (pandas.core.frame.DataFrame): El DataFrame al que se agregará la columna 'dormida'.
    - cluster_val (Any): El valor que debe coincidir en la columna 'cluster'.
    - start_time (int): La hora de inicio del rango de tiempo (en formato de 24 horas).
    - end_time (int): La hora de finalización del rango de tiempo (en formato de 24 horas).

    Retorna:
    -----------
    - df (pd.DataFrame): El DataFrame modificado con la nueva columna 'dormida'.

    """
    # Agrega una nueva columna llamada 'dormida' al DataFrame y le asigna un valor 'NO' por defecto.
    df["dormida"] = "NO"

    # Itera sobre cada fila en el DataFrame y comprueba si el valor en la columna 'cluster'
    # coincide con el valor 'cluster_val' dado y la hora en la columna 'point_ini' está dentro
    # del rango de tiempo dado. Si es así, cambia el valor de la columna 'dormida' a 'SI'.
    for i, row in df.iterrows():
        if row["cluster"] == cluster_val:
            hora = pd.to_datetime(row["point_ini"]) - pd.Timedelta(hours=3)
            if hora.hour >= start_time or hora.hour < end_time:
                df.loc[i, "dormida"] = "SI"

    # Devuelve el DataFrame modificado con la nueva columna 'dormida'.
    return df


def transform_decimal_hour(numero_horas: float) -> str:
    """
    Esta función toma un número de horas y lo convierte a una cadena de caracteres
    en el formato de "h, min, seg".

    Parámetros:
    -----------
    - numero_horas (float): El número de horas que se desea convertir.

    Retorna:
    -----------
    - str (str): Una cadena de caracteres en el formato de "h, min, seg".

    """
    # Convierte el número de horas a números enteros de horas, minutos y segundos.
    horas = int(numero_horas)
    minutos = int((numero_horas - horas) * 60)
    segundos = int(((numero_horas - horas) * 60 - minutos) * 60)

    # Devuelve una cadena de caracteres en el formato de "h, min, seg".
    return f"{horas} h, {minutos} min, {segundos} seg"


def acumular_diferencia_tiempo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el tiempo total que un animal pasa durmiendo, pastando, rumiando o bebiendo según las condiciones
    dadas en el DataFrame de entrada. Retorna un nuevo DataFrame con los valores totales de cada actividad.

    Parámetros:
    -----------
    - df (pd.DataFrame): DataFrame con las columnas "point_ini", "point_next", "dormida", "cluster" y "agua".

    Retorna:
    -----------
    - total_df (pd.DataFrame): DataFrame con las columnas "rumiando", "pastando", "durmiendo", "bebiendo" y "cant_registro".

    """

    # Convertir las columnas "point_ini" y "point_next" en valores de tipo datetime
    df["point_ini"] = pd.to_datetime(df["point_ini"])
    df["point_next"] = pd.to_datetime(df["point_next"])

    # Crear las columnas "rumeando", "pastando", "durmiendo", "bebiendo" y establecer el valor inicial a 0
    df["rumeando"] = 0
    df["pastando"] = 0
    df["durmiendo"] = 0
    df["bebiendo"] = 0
    cantidadregistro = 0

    # Recorrer el DataFrame y sumar los valores de la diferencia entre "point_ini" y "point_next" según las condiciones dadas
    for i, row in df.iterrows():
        if row["dormida"] == "SI" and row["agua"] == 0:
            df.at[i, "durmiendo"] += (
                (row["point_next"] - row["point_ini"]).total_seconds()
            ) / 3600
        elif row["cluster"] == 1 and row["dormida"] == "NO" and row["agua"] == 0:
            df.at[i, "rumeando"] += (
                (row["point_next"] - row["point_ini"]).total_seconds()
            ) / 3600
        elif row["cluster"] == 0 and row["agua"] == 0:
            df.at[i, "pastando"] += (
                (row["point_next"] - row["point_ini"]).total_seconds()
            ) / 3600
        elif row["agua"] == 1:
            df.at[i, "bebiendo"] += (
                (row["point_next"] - row["point_ini"]).total_seconds()
            ) / 3600
        cantidadregistro += 1

    # Crear un nuevo DataFrame con los valores totales de cada actividad
    total_df = pd.DataFrame(
        {
            "rumiando": [transform_decimal_hour(df["rumeando"].sum())],
            "pastando": [transform_decimal_hour(df["pastando"].sum())],
            "durmiendo": [transform_decimal_hour(df["durmiendo"].sum())],
            "bebiendo": [transform_decimal_hour(df["bebiendo"].sum())],
            "cant_registro": cantidadregistro,
        }
    )

    return total_df


def separador_por_dia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Esta función toma un DataFrame y devuelve un nuevo DataFrame que contiene las diferencias de tiempo acumuladas por día
    a partir de la columna 'point_ini'. El DataFrame de entrada debe contener la columna 'point_ini'.

    Parámetros:
    -----------
    - df (pd.DataFrame): El DataFrame de entrada con la columna 'point_ini' que contiene fechas y horas.

    Retorna:
    --------
    - diarios (pd.DataFrame): un nuevo DataFrame con la fecha en la columna de índice y las diferencias de tiempo acumuladas en las otras columnas.
    """
    df["fecha"] = pd.to_datetime(df["point_ini"]).dt.date

    diarios: Dict[Union[str, Any], pd.DataFrame] = {}
    for fecha, grupo in df.groupby(df["point_ini"].dt.date):
        diarios[fecha] = acumular_diferencia_tiempo(grupo)

    diarios = pd.concat(diarios.values(), keys=diarios.keys(), axis=0)
    diarios = diarios.reset_index(level=1).drop(columns=["level_1"])
    df = df.drop(columns=['fecha'])
    return diarios


# DIAGNOSTICO -------------------------------------------------


def respuesta_diagnostico(valor: float, min: float, max: float) -> str:
    """
    Esta función devuelve una respuesta de diagnóstico según el valor dado, el valor mínimo y el valor máximo permitidos.

    Parámetros:
    -----------
    - valor (float): el valor que se quiere diagnosticar.
    - min (float): el valor mínimo permitido.
    - max (float): el valor máximo permitido.

    Retorna:
    --------
    - result (str):la respuesta de diagnóstico según el valor dado y los valores mínimo y máximo permitidos.
    """
    if valor > min and valor < (max + (max * 0.05)):
        result = "normal"
    elif valor > (min - (min * 0.25)) and valor < (max + (max * 0.25)):
        result = "atencion!"
    else:
        result = "mal"

    return result


def diagnostico_devices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Esta función devuelve un marco de datos que contiene diagnósticos para diferentes dispositivos, como rumia, pastoreo,
    durmiendo, agua y cantidad de registros, en función de los valores de entrada.

    Parámetros:
    -----------
    - df (pd.DataFrame): el marco de datos de entrada que contiene información sobre el comportamiento de los dispositivos.

    Retorna:
    --------
    - diag (pd.DataFrame): el marco de datos que contiene diagnósticos para diferentes dispositivos.
    """
    rumia = [float(x.split("h")[0]) for x in df["rumiando"]]
    pastoreo = [float(x.split("h")[0]) for x in df["pastando"]]
    durmiendo = [float(x.split("h")[0]) for x in df["durmiendo"]]
    agua = [float(x.split("h")[0]) for x in df["bebiendo"]]
    can_r = [
        "optimo" if x >= 72 else "poco" if x < 68 else "no optimo"
        for x in df["cant_registro"]
    ]

    diag = pd.DataFrame(
        {
            "fecha": [x for x in df.index],
            "rumiando": [respuesta_diagnostico(x, 6, 8) for x in rumia],
            "pastando": [respuesta_diagnostico(x, 8, 12) for x in pastoreo],
            "durmiendo": [respuesta_diagnostico(x, 5, 8) for x in durmiendo],
            "agua": [respuesta_diagnostico(x, 1, 4) for x in agua],
            "cant_registro": can_r,
        }
    )

    return diag


def process_and_transform_data(df: pd.DataFrame,nombre : str, id : str, fecha_init: str):
    finca = filter_area_perimetro(df,nombre)
    df_gp= filter_devices(finca,id)
    df_gp = select_data_by_date(df_gp,fecha_init)
    df_gp = dataframe_interview_vaca(df_gp)
    df_gp = agregar_iths(df_gp,nombre)
    df_gp = predict_model(df_gp)
    d= agua_click(df_gps, id, fecha_init,nombre )
    df_gp = result_select(df_gp,d)
    df_gp = add_dormida_column(df_gp, 1, 20, 6)
    return df_gp

def process_and_transform_datas(df: pd.DataFrame,nombre : str, id : str, fecha_init: str, fecha_fin:str):
    finca = filter_area_perimetro(df,nombre)
    df_gp= filter_devices(finca,id)
    df_gp = select_data_by_dates(df_gp,fecha_init,fecha_fin)
    df_gp = dataframe_interview_vaca(df_gp)
    df_gp = agregar_iths(df_gp,nombre)
    df_gp = predict_model(df_gp)
    d= agua_clicks(df_gps, id, nombre, fecha_init, fecha_fin)
    df_gp = result_select(df_gp,d)
    df_gp = add_dormida_column(df_gp, 1, 20, 6)
    return df_gp

def resumen_and_diagnostic(df):
    data = separador_por_dia(df)
    diagnostic= diagnostico_devices(data)
    df=df.drop(columns=['fecha'])
    return data , diagnostic
