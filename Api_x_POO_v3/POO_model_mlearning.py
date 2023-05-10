import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from pandas import Series
from numpy import ndarray


class Detected_Activiti_Devices:
    
    def __dataframe_entrenamiento(self) -> pd.DataFrame:
        """
        Genera un DataFrame que contiene datos sintéticos para entrenar un modelo de clasificación de actividades de una vaca.
        El DataFrame resultante contiene columnas para la distancia recorrida, velocidad, tiempo total, aceleración y la
        actividad de la vaca (pastoreo o rumia).

        Retorna:
        -----------
        - concatenado (pd.DataFrame): un DataFrame con datos sintéticos para entrenar un modelo de clasificación de actividades de una vaca.
        """
        # DataFrame para pastoreo
        pastoreo_df = pd.DataFrame(
            {
                "distancia": np.random.normal(loc=0.025, scale=0.01, size=7000),
                "velocidad": np.random.normal(loc=0.2, scale=0.05, size=7000),
                "tiempo": np.random.normal(loc=0.15, scale=0.05, size=7000),
                "aceleracion": np.random.normal(loc=-0.2, scale=0.1, size=7000),
                "actividad": "pastoreo",
            }
        )

        # DataFrame para rumia
        rumia_df = pd.DataFrame(
            {
                "distancia": np.random.normal(loc=0.005, scale=0.002, size=7000),
                "velocidad": np.random.normal(loc=0.01, scale=0.002, size=7000),
                "tiempo": np.random.normal(loc=0.5, scale=0.05, size=7000),
                "aceleracion": np.random.normal(loc=-0.05, scale=0.02, size=7000),
                "actividad": "rumia",
            }
        )

        concatenado = pd.concat([pastoreo_df, rumia_df], axis=0, ignore_index=True)
        concatenado = concatenado.sample(frac=1, random_state=42).reset_index(drop=True)
        cambio = {"pastoreo": 0, "rumia": 1}
        concatenado.actividad = concatenado.actividad.map(cambio)

        return concatenado


    def __fit_model__(self) -> KMeans:
        """
        Entrena un modelo KMeans con datos de entrenamiento escalados con StandardScaler.

        Retorna:
        -----------
        - kmeans (KMeans): un objeto KMeans entrenado con los datos escalados.
        """
        concatenado: pd.DataFrame = self.__dataframe_entrenamiento()
        scaler: StandardScaler = StandardScaler()
        data_sca: ndarray = scaler.fit_transform(concatenado[["velocidad", "aceleracion"]])
        y: Series = concatenado["actividad"]
        kmeans: KMeans = KMeans(n_clusters=2, random_state=42)
        kmeans.fit(data_sca, y)
        return kmeans


    def predict_model(self,data: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza predicciones utilizando el modelo KMeans previamente entrenado.

        Parámetros:
        -----------
        - data (pd.DataFrame): un DataFrame que contiene las características de los datos de prueba.

        Retorna:
        -----------
        - data (pd.DataFrame): un DataFrame que contiene las características de los datos de prueba y una columna con las predicciones del modelo.
        """
        kmeans =self.__fit_model__()
        data = data.fillna(0.0)
        data.loc[(data.aceleracion == np.inf) | (data.aceleracion == -np.inf),'aceleracion']=0.0
        x_test = data[['velocidad','aceleracion']].values
        perro = kmeans.predict(x_test)
        data['cluster'] = perro
        dist_1=data[data['cluster']== 1].distancia.sum()
        dist_0=data[data['cluster']== 0].distancia.sum()
        if dist_1 < dist_0 : data['cluster'] = data['cluster'].map({1:0,0:1})  
        return data
