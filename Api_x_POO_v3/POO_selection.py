import pandas as pd
import geopandas as gpd
import math
from shapely.geometry import Point
import pymongo



class Select_Data_for_Process:
    __dataframe:pd.DataFrame
    __url_conexion__:str="localhost:27017"
    __conexion  = pymongo.MongoClient(__url_conexion__)
    __db =__conexion["test"]
    __dataframe:pd.DataFrame 
    setle:str
    
    
    def __dataframe_GPS__(self,type_date:str = 'GPS') -> pd.DataFrame:
        rows = self.__db["datarows"]
        data_row = rows.find({"dataRowType": type_date})
        df_row = pd.json_normalize(data_row, sep="_")
        df_row._id = df_row._id.astype(str)
        columns= ["UUID","dataRowType","createdAt",
                "updatedAt","dataRowData_lat","dataRowData_lng",
                "dataRowData_gpsAlt","dataRowData_gpsVel","dataRowData_gpsFixed",]
        if type_date == 'GPS': df_row.drop(df_row[df_row.dataRowData_lat.isna()].index, inplace=True)
        df_row = df_row[columns].reset_index()
        return df_row
    
    def __mongo_data_collection__(self,collection: str) -> pd.DataFrame:
        mongoColle = self.__db[collection]
        data = list(mongoColle.find())
        df: pd.DataFrame = pd.json_normalize(data, sep="_")
        df._id = df._id.astype(str)
        return df
    
    def __setle_select(self,select: str) -> pd.DataFrame:
        data= self.__db['settlements']
        obj= data.find({'name':select})
        df_setle= pd.json_normalize(obj)
        df_setle._id = df_setle._id.astype(str)
        df_setle['latitud_c']=df_setle.centralPoint.apply(lambda x: x[0]['lat'] if 'lat' in x[0] else None)
        df_setle['longitud_c']=df_setle.centralPoint.apply(lambda x: x[0]['lng'] if 'lng' in x[0] else None)
        setle_n = df_setle[['_id','hectares','registerNumber','headsCount','name','latitud_c','longitud_c']]
        return setle_n

    def __get_dataframe__(self):
        return self.__dataframe
    

    def __perimetro_aprox(self,hectarea: float) -> float:
        hect = hectarea  # Asignamos el valor del parámetro hectarea a la variable hect
        lado = (math.sqrt(hect) * 10)  # Calculamos la longitud del lado de un cuadrado cuya área es igual a hect y multiplicamos por 10
        perim = (lado * 4)  # Calculamos el perímetro del cuadrado multiplicando la longitud del lado por 4

        return perim

    def __conect_aguada_data_devices(self) -> pd.DataFrame:
        df_animal: pd.DataFrame = self.__mongo_data_collection__("animals")
        df_animal["animalSettlement"] = df_animal["animalSettlement"].apply(lambda x: x[0])
        df_animal.animalSettlement = df_animal.animalSettlement.astype(str)
        result: pd.DataFrame = df_animal[(df_animal.caravanaNumber.str.contains("AGUADA")) | (df_animal.caravanaNumber.str.contains("PUNTO_FIJO"))]
        return result

    def select_data_by_date(self,fecha: str) -> pd.DataFrame:
        data = self.__dataframe
        if "createdAt" not in data.columns:
            raise ValueError("El DataFrame no contiene la columna 'createdAt'")
        try:
            fecha_deseada = pd.to_datetime(fecha)
        except ValueError:
            raise ValueError("La variable 'fecha' debe estar en el formato 'YYYY-MM-DD'")
        nuevo_df = data.loc[data["createdAt"].dt.date == fecha_deseada.date()]
        return nuevo_df


    def select_data_by_dates(self, fecha_init: str, fecha_fin: str) -> pd.DataFrame:
        data = self.__dataframe
        if "createdAt" not in data.columns:
            raise ValueError("El DataFrame no contiene la columna 'createdAt'")
        data["createdAt"] = pd.to_datetime(data["createdAt"])
        try:
            fecha_deseada1 = pd.to_datetime(fecha_init).date()
            fecha_deseada2 = pd.to_datetime(fecha_fin).date()
        except ValueError:
            raise ValueError("La variable 'fecha' debe estar en el formato 'YYYY-MM-DD'")
        nuevo_df = data[(data["createdAt"].dt.date >= fecha_deseada1)& (data["createdAt"].dt.date <= fecha_deseada2)]
        return nuevo_df
    
    def get_aguada(self,setle: str= None) -> pd.DataFrame:       
        if setle == None :setle= self.setle._id.values[0]
        df_devis= self.__mongo_data_collection__('devices')
        df_devis.deviceAnimalID=df_devis.deviceAnimalID.astype(str)
        data_devise = df_devis[df_devis.deviceType=='PUNTO FIJO'] 
        aguadas= self.__conect_aguada_data_devices()
        aguadas.animalSettlement = aguadas.animalSettlement.apply(lambda x:str(x)) 
        x= aguadas[aguadas.animalSettlement == setle]
        agua =data_devise[data_devise.deviceAnimalID.isin(x._id.values)]
        return agua
    
    def filter_devices(self, uuid: str = None) -> pd.DataFrame:
        data = self.__dataframe
        data = data[data.UUID == uuid]
        data.drop(data[data.dataRowData_lat.isna()].index, inplace=True)
        data.reset_index()
        return data
    
    def filter_area_perimetro(self,setle:str):
        setle= self.__setle_select(setle)
        data = self.__dataframe
        gdf= gpd.GeoDataFrame(data,crs='EPSG:4326',geometry=gpd.points_from_xy(data.dataRowData_lng,data.dataRowData_lat))
        setle_lat=setle['latitud_c'].values[0]
        setle_lng=setle['longitud_c'].values[0]
        hectareas=setle['hectares'].values[0]
        punto_referencia= Point(setle_lng,setle_lat)	
        per_kilo= self.__perimetro_aprox(hectareas)
        circulo= punto_referencia.buffer(per_kilo/111.32) # valor 1 grado aprox en kilometro en el ecuador 
        on_perimetro= gdf[gdf.geometry.within(circulo)]
        agua = self.get_aguada(setle._id.values[0])
        on_perimetro = on_perimetro.drop(on_perimetro[on_perimetro['UUID'].isin(agua.deviceMACAddress.unique())].index)
        return on_perimetro
    
    def __init__(self,nombre:str= None, uuid:str= None ,fecha_ini:str= None, fecha_finsh:str= None,type_date:str= None):
        if type_date == None: self.__dataframe = self.__dataframe_GPS__()
        else: self.__dataframe = self.__dataframe_GPS__(type_date)
        if nombre != None:
            self.__dataframe = self.filter_area_perimetro(nombre)
            self.setle = self.__setle_select(nombre)
        if uuid != None: self.__dataframe = self.filter_devices(uuid)
        if uuid != None: self.__dataframe = self.filter_devices(uuid)
        if fecha_ini!= None and fecha_finsh == None: self.__dataframe = self.select_data_by_date(fecha_ini)
        if fecha_ini!= None and fecha_finsh != None: self.__dataframe = self.select_data_by_dates(fecha_ini,fecha_finsh)


