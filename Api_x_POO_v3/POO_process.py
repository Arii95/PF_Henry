import pandas as pd
from geopy.distance import great_circle
import geopandas as gpd
import math
from shapely.geometry import Point
from typing import Dict, Any
from POO_selection import Select_Data_for_Process
from POO_model_mlearning  import Detected_Activiti_Devices


class Process_Data:
    __dataframe:Select_Data_for_Process 
    __dataframe_process__:pd.DataFrame
    setle:str
    model=Detected_Activiti_Devices()
    aguadas:Select_Data_for_Process.get_aguada
    
    
    def get_dataframe_process(self):
        return self.__dataframe_process__

    def __perimeter_aprox__(self,hectarea: float) -> float:
        hect = hectarea  # Asignamos el valor del parámetro hectarea a la variable hect
        lado = (math.sqrt(hect) * 10)  # Calculamos la longitud del lado de un cuadrado cuya área es igual a hect y multiplicamos por 10
        perim = (lado * 4)  # Calculamos el perímetro del cuadrado multiplicando la longitud del lado por 4
        return perim

    def __get_records_near_drinker(self, data, latitud: float, longitud: float, metro: float) -> gpd.GeoDataFrame:
    
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
        ) 
        # Filtrar los puntos que están dentro del círculo
        on_perimetro = gdf[gdf.geometry.within(circulo)]
        # Devolver el subconjunto de datos filtrados
        return on_perimetro
    
    def __transform_data(self,data: pd.DataFrame) -> pd.DataFrame:
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
                    data_dis.append(dista_km)
                if data.iloc[i].dataRowData_gpsVel:
                    data_vel.append(round(data.iloc[i].dataRowData_gpsVel, 3))
                    data_time.append(round(dista_km / data.iloc[i].dataRowData_gpsVel, 3))
                else:
                    data_time.append(round(dista_km / pd.Series(data_vel).mean().round(3), 3))  # les puede dar error si el array de velocidad esta vacio... toma el valor promedio de las velocidades que hay hasta el momento
            except IndexError:
                pass
        df = list(zip(data_in, data_fin, data_inter, data_dis, data_vel, data_time))
        df = pd.DataFrame(df,columns=["point_ini","point_next","interval_time","distancia","velocidad","tiempo",])
        df["aceleracion"] = df["velocidad"].diff() / df["tiempo"].diff()
        return df
    
    def __indicted_and_tranform_data(self,datos:pd.DataFrame= None) -> pd.DataFrame:
        if datos == None: datos = self.__dataframe.__get_dataframe__()
        vacas = datos.UUID.unique()
        data_nuevo: Dict[str, Any] = {}
        for i in vacas:
            data = datos[datos.UUID == i ]
            data_nuevo[i] =self.__transform_data(data)
        merge_data = pd.concat(data_nuevo.values(), keys=data_nuevo.keys())
        merge_data.reset_index(level=0, inplace=True)
        merge_data.rename(columns={"level_0": "UUID"}, inplace=True)
        merge_data.reset_index(inplace=True)
        merge_data.set_index("UUID")
        merge_data.drop(columns="index", inplace=True)
        return merge_data

    
    def __agregar_iths(self,data) -> pd.DataFrame:
        try:
            setle= self.__dataframe.setle
            df_setith= self.__dataframe.__mongo_data_collection__('settlementithcounts')
            df_setith.settlementId =df_setith.settlementId.astype(str)
            pru= df_setith[df_setith.settlementId == setle._id.values[0]]
            aux= {}
            for fecha in data.point_ini.dt.date.unique():
                prueba=pru[pru.createdAt.dt.date== fecha]
                dataq=data[data.point_ini.dt.date  == fecha]
                aux[fecha]= pd.merge(dataq,prueba[['createdAt', 'ITH']],left_on=dataq.point_ini.dt.hour, right_on=prueba['createdAt'].dt.hour)
            prueb= pd.concat(aux.values())  
            prueb= prueb.drop(columns=['key_0','createdAt'])  
            if prueb.shape[0] == 0:
                data['ITH'] = 'S/N'
                return data
            return prueb
        except AttributeError:
            data['ITH'] = 'ERROR'
            return data

    def __add_asleep_column(self,data, start_time: int=21, end_time: int=6) -> pd.DataFrame:
        data["dormida"] = "NO"
        for i, row in data.iterrows():
            if row["cluster"] == 1:
                hora = pd.to_datetime(row["point_ini"]) - pd.Timedelta(hours=3)
                if hora.hour >= start_time or hora.hour < end_time:
                    data.loc[i, "dormida"] = "SI"
        return data


    def gps_aguada(self) -> pd.DataFrame:
        aguadas = self.__dataframe.get_aguada()
        df = self.__dataframe.__dataframe_GPS__()
        # Filtrar los datos para incluir solo los dispositivos que se utilizaron para registrar aguadas
        movi_agu = df[df.UUID.isin(aguadas.deviceMACAddress.unique())]

        # Inicializar un diccionario para almacenar las últimas ubicaciones conocidas de cada dispositivo
        data = {}

        # Para cada dispositivo, encontrar su última ubicación conocida y agregarla al diccionario
        for i in aguadas.deviceMACAddress:
            data_de = movi_agu[movi_agu.UUID== i]
            data[i] = data_de.iloc[-1][["dataRowData_lat", "dataRowData_lng"]]

        # Crear un DataFrame a partir del diccionario y devolverlo
        dtf = pd.DataFrame(data).transpose()
        return dtf


    def agua_click(self, vaca: str, fecha: str=None,fecha_fin:str= None) -> pd.DataFrame:
        data = Select_Data_for_Process(nombre=self.__dataframe.setle.name.values[0],fecha_ini=fecha,fecha_finsh=fecha_fin)#.__get_dataframe__()aguadas= self.process.get_aguada()
        data = data.__get_dataframe__()
        dtf= self.gps_aguada()
        prueba= {}
        for i,d in dtf.iterrows():
            prueba[i]=self.__get_records_near_drinker(data,d['dataRowData_lat'] , d['dataRowData_lng'],4.6)
        prueb=pd.concat(prueba.values())
        p= prueb[prueb.UUID==vaca]
        return p


    def result_select(self, data_values: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
        
        # Identificar los registros cuya hora de inicio coincide con la hora de creación de un registro en "data".
        select = data_values.point_ini.dt.strftime("%H:%M").isin(data.createdAt.dt.strftime("%H:%M").values)

        # Asignar un valor de 1 a la columna "agua" para los registros seleccionados y llenar los valores faltantes con 0.
        data_values.loc[select, "agua"] = 1
        data_values.agua = data_values.agua.fillna(0)

        return data_values
    
    def process_and_transform_data(self, uuid : str, fecha_init: str=None,fecha_fin:str=None):
        data = self.__indicted_and_tranform_data()
        data= self.__agregar_iths(data)
        data = self.model.predict_model(data)
        d= self.agua_click(uuid, fecha_init,fecha_fin)
        data = self.result_select(data,d)
        data = self.__add_asleep_column(data,20, 6)
        return data
    
    def __init__(self,nombre:str = None,uuid:str = None,fecha_ini:str = None,fecha_finsh:str = None) -> None:
        self.__dataframe= Select_Data_for_Process(nombre,uuid,fecha_ini,fecha_finsh)
        if nombre != None and uuid== None and fecha_ini== None and fecha_finsh== None: self.__dataframe_process__ = self.__indicted_and_tranform_data()
        if uuid != None:
            self.__dataframe_process__ = self.process_and_transform_data(uuid,fecha_ini,fecha_finsh)
            self.aguadas= self.__dataframe.get_aguada()






