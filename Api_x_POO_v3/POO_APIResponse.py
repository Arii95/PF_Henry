import pandas as pd
from POO_process import Process_Data
from typing import Dict, Any, Union



class APIResult:
    data: pd.DataFrame
    resumen:pd.DataFrame
    diagnostic:pd.DataFrame
    
    def __transform_decimal_hour__(self,numero_horas: float) -> str:

        # Convierte el número de horas a números enteros de horas, minutos y segundos.
        horas = int(numero_horas)
        minutos = int((numero_horas - horas) * 60)
        segundos = int(((numero_horas - horas) * 60 - minutos) * 60)

        # Devuelve una cadena de caracteres en el formato de "h, min, seg".
        return f"{horas} h, {minutos} min, {segundos} seg"


    def __acumular_diferencia_tiempo(self,df) -> pd.DataFrame:
        #
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
                "rumiando": [self.__transform_decimal_hour__(df["rumeando"].sum())],
                "pastando": [self.__transform_decimal_hour__(df["pastando"].sum())],
                "durmiendo": [self.__transform_decimal_hour__(df["durmiendo"].sum())],
                "bebiendo": [self.__transform_decimal_hour__(df["bebiendo"].sum())],
                "cant_registro": cantidadregistro,
            }
        )

        return total_df


    def resumen_data(self) -> pd.DataFrame:
        df = self.data
        df["fecha"] = pd.to_datetime(df["point_ini"]).dt.date

        diarios: Dict[Union[str, Any], pd.DataFrame] = {}
        for fecha, grupo in df.groupby(df["point_ini"].dt.date):
            diarios[fecha] =self.__acumular_diferencia_tiempo(grupo)

        diarios = pd.concat(diarios.values(), keys=diarios.keys(), axis=0)
        diarios = diarios.reset_index(level=1).drop(columns=["level_1"])
        return diarios



    def __respuesta_diagnostico__(self,valor: float, min: float, max: float) -> str:
            
            if valor > min and valor < (max + (max * 0.05)):
                result = "normal"
            elif valor > (min - (min * 0.25)) and valor < (max + (max * 0.25)):
                result = "atencion!"
            else:
                result = "mal"

            return result


    def diagnostico_devices(self,df:pd.DataFrame = None) -> pd.DataFrame:
            df= self.resumen
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
                    "rumiando": [self.__respuesta_diagnostico__(x, 6, 8) for x in rumia],
                    "pastando": [self.__respuesta_diagnostico__(x, 8, 12) for x in pastoreo],
                    "durmiendo": [self.__respuesta_diagnostico__(x, 5, 8) for x in durmiendo],
                    "agua": [self.__respuesta_diagnostico__(x, 1, 4) for x in agua],
                    "cant_registro": can_r,
                }
            )

            return diag
    
    def respuesta_api(self):
        datos = self.data[['UUID','point_ini','point_next','distancia','velocidad','tiempo','aceleracion','ITH', 'cluster', 'agua','dormida']]
        resumen = self.resumen
        diagnostic = self.diagnostic
        resumen.index= resumen.index.astype(str)
        datos.point_ini= datos.point_ini.astype(str)
        datos.point_next= datos.point_next.astype(str)
        diagnostic.fecha= diagnostic.fecha.astype(str)
        return datos,resumen,diagnostic
    
    def __init__(self,nombre:str = None,uuid:str = None,fecha_ini:str = None,fecha_finsh:str = None):
        self.data = Process_Data(nombre,uuid,fecha_ini,fecha_finsh).get_dataframe_process() 
        self.resumen = self.resumen_data()
        self.diagnostic = self.diagnostico_devices()
        self.data = self.data.drop(columns=['fecha'])







