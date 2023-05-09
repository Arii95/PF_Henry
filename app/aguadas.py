import pandas as pd
import geopandas as gpd
import geopandas as gpd
import math
from typing import List, Dict, Any, Union
from geopy import Point
from shapely.geometry import Point

from support_api import (
    select_data_by_date,
    select_data_by_dates,
    data_devices,
    update_aguada,
)


def filter_area_peri(
    data: List[Dict[str, Any]], latitud: float, longitud: float, metro: float
) -> gpd.GeoDataFrame:
    """
    Esta función filtra un subconjunto de datos que están dentro de un círculo con el radio especificado alrededor de las coordenadas dadas.

    Parámetros:
    -----------
    - data (List[Dict[str, Any]]): Una lista de diccionarios, donde cada diccionario representa una fila de datos.
    - latitud (float): Una coordenada de latitud como un número de punto flotante.
    - longitud (float): Una coordenada de longitud como un número de punto flotante.
    - metro (float): El radio del círculo en metros, como un número de punto flotante.

    Retorna:
    -----------
    - on_perimetro (gpd.GeoDataFrame): Un subconjunto de los datos de entrada que se encuentran dentro del círculo, representado como un GeoDataFrame.
    """

    # Crear un GeoDataFrame a partir de los datos de entrada
    gdf = gpd.GeoDataFrame(
        data,
        crs="EPSG:4326",
        geometry=gpd.points_from_xy(data.dataRowData_lng, data.dataRowData_lat),
    )

    # Establecer las coordenadas de referencia
    setle_lat = latitud
    setle_lng = longitud
    punto_referencia = Point(setle_lng, setle_lat)

    # Calcular el radio del círculo
    per_kilo = math.sqrt(metro) * 0.01
    circulo = punto_referencia.buffer(
        per_kilo / 111.32
    )  # valor 1 grado aprox en kilometro en el ecuador

    # Filtrar los puntos que están dentro del círculo
    on_perimetro = gdf[gdf.geometry.within(circulo)]

    # Devolver el subconjunto de datos filtrados
    return on_perimetro


def gps_aguada(aguadas: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """
    Esta función recibe dos DataFrames como entrada y devuelve un subconjunto de los datos que corresponden a las últimas ubicaciones conocidas de los dispositivos que se utilizaron para registrar aguadas.

    Parámetros:
    -----------
    - aguadas (pd.DataFrame): Un DataFrame que contiene los datos de las aguadas registradas, con una columna llamada 'deviceMACAddress' que contiene los identificadores de los dispositivos utilizados.
    - df (pd.DataFrame): Un DataFrame que contiene los datos de localización, con una columna llamada 'UUID' que contiene los identificadores de los dispositivos.

    Retorna:
    -----------
    - dtf (pd.DataFrame): Un DataFrame que contiene las últimas ubicaciones conocidas de los dispositivos que se utilizaron para registrar aguadas, con una fila por dispositivo y columnas para latitud y longitud.
    """

    # Filtrar los datos para incluir solo los dispositivos que se utilizaron para registrar aguadas
    movi_agu = df[df.UUID.isin(aguadas.deviceMACAddress.unique())]

    # Inicializar un diccionario para almacenar las últimas ubicaciones conocidas de cada dispositivo
    data = {}

    # Para cada dispositivo, encontrar su última ubicación conocida y agregarla al diccionario
    for i in aguadas.deviceMACAddress:
        data_de = data_devices(movi_agu, i)
        print(data_de.shape)
        data[i] = data_de.iloc[-1][["dataRowData_lat", "dataRowData_lng"]]

    # Crear un DataFrame a partir del diccionario y devolverlo
    dtf = pd.DataFrame(data).transpose()
    return dtf


def agua_click(data: pd.DataFrame, vaca: str, fecha: str, setle: str) -> pd.DataFrame:
    """
    Esta función recibe un DataFrame de datos de localización, un identificador de vaca, una fecha y un asentamiento como entrada, y devuelve los datos de localización para la vaca en la fecha especificada y dentro del área de un círculo de 4 km de radio centrado en la última ubicación conocida de los dispositivos utilizados para registrar aguadas en el asentamiento especificado.

    Parámetros:
    -----------
    - data (pd.DataFrame): Un DataFrame que contiene los datos de localización.
    - vaca (str): El identificador de la vaca cuyos datos se desean obtener.
    - fecha (str): La fecha para la cual se desean obtener los datos de localización.
    - setle (str): El identificador del asentamiento para el cual se deben considerar las últimas ubicaciones conocidas de los dispositivos utilizados para registrar aguadas.

    Retorna:
    -----------
    - p (pd.DataFrame): Un DataFrame que contiene los datos de localización para la vaca especificada en la fecha especificada y dentro del área de un círculo de 4 km de radio centrado en la última ubicación conocida de los dispositivos utilizados para registrar aguadas en el asentamiento especificado.
    """

    # Obtener los datos actualizados de las aguadas para el asentamiento especificado
    aguadas = update_aguada(setle)
    print(aguadas.shape)

    # Encontrar la última ubicación conocida de los dispositivos utilizados para registrar aguadas y filtrar los datos de localización para incluir solo los que se encuentran dentro del área de un círculo de 4 km centrado en esa ubicación
    dtf = gps_aguada(aguadas, data)
    print(dtf.iloc[0, 0])
    data_p = filter_area_peri(data, dtf.iloc[0, 0], dtf.iloc[0, 1], 4.0)

    # Filtrar los datos de localización para la vaca especificada en la fecha especificada y devolverlos
    day_p = select_data_by_date(data_p, fecha)
    p = data_devices(day_p, vaca)
    return p


def agua_clicks(
    data: List[Dict[str, Any]],
    vaca: str,
    fecha: str,
    fecha2: str,
    setle: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Filtra los datos de un conjunto de dispositivos GPS para una vaca específica y un rango de fechas dado,
    seleccionando solo los datos que corresponden a los periodos en los que la vaca visitó un bebedero.

    Parámetros:
    -----------
    - data (List[Dict[str, Any]]): una lista de diccionarios que contiene los datos de ubicación de los dispositivos GPS.
    - vaca (str): una cadena de caracteres que representa el identificador de la vaca para la cual se desean filtrar los datos.
    - fecha (str): una cadena de caracteres que representa la fecha de inicio del rango de fechas que se desea filtrar.
    - fecha2 (str): una cadena de caracteres que representa la fecha de fin del rango de fechas que se desea filtrar.
    - setle (Dict[str, Any]): un diccionario que contiene la información necesaria para identificar los bebederos visitados por la vaca,
            incluyendo las coordenadas de cada bebedero y la distancia máxima permitida para considerar una visita.

    Retorna:
    -----------
    - p (List[Dict[str, Any]]): una lista de diccionarios que contienen los datos de ubicación de la vaca especificada en los periodos en los que
      la vaca visitó algún bebedero del conjunto de bebederos especificado.
    """
    # Actualizar información de los bebederos y obtener los periodos en los que la vaca visitó algún bebedero.
    aguadas = update_aguada(setle)
    dtf = gps_aguada(aguadas, data)

    # Filtrar los datos de ubicación de la vaca en el área del primer bebedero visitado.
    data_p = filter_area_peri(data, dtf.iloc[0, 0], dtf.iloc[0, 1], 4.0)

    # Seleccionar los datos de ubicación de la vaca dentro del rango de fechas especificado.
    day_p = select_data_by_dates(data_p, fecha, fecha2)

    # Seleccionar los datos de la vaca especificada y retornarlos.
    p = data_devices(day_p, vaca)
    return p


def result_select(data_values: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asigna un valor de 1 a la columna "agua" del diccionario "data_values" para aquellos registros cuya hora de inicio
    coincide con la hora de creación de un registro en el diccionario "data", y un valor de 0 para los demás registros.

    Parámetros:
    -----------
    - data_values (Dict[str, Any]): un diccionario que contiene los registros de las mediciones realizadas en un bebedero, incluyendo la
                   hora de inicio y fin de cada registro, la cantidad de litros de agua consumidos y una columna que
                   indica si se detectó actividad en el bebedero en el periodo de tiempo correspondiente.
    - data (Dict[str, Any]): un diccionario que contiene los registros de ubicación de los dispositivos GPS, incluyendo la hora de creación
            de cada registro.

    Retorna:
    -----------
    - data_values (Dict[str, Any]): un diccionario actualizado que contiene los mismos registros que el diccionario de entrada, pero con la columna
      "agua" actualizada para indicar si se detectó actividad en el bebedero en el periodo de tiempo correspondiente.
    """
    # Identificar los registros cuya hora de inicio coincide con la hora de creación de un registro en "data".
    select = data_values.point_ini.dt.strftime("%H:%M").isin(
        data.createdAt.dt.strftime("%H:%M").values
    )

    # Asignar un valor de 1 a la columna "agua" para los registros seleccionados y llenar los valores faltantes con 0.
    data_values.loc[select, "agua"] = 1
    data_values.agua = data_values.agua.fillna(0)

    return data_values
