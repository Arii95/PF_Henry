import pandas as pd
import numpy as np
from sklearn.cluster import KMeans


# Dataframe para pastoreo
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


X = concatenado[['velocidad','aceleracion']]#'distancia',
y= concatenado['actividad']

# crear el modelo de K-means con 2 clusters para (rumia y pastoreo)
kmeans = KMeans(n_clusters=2,random_state=0).fit(X,y)

# funcion que utiliza el modelo entrenado.. "identifica el comprtamiento" y indica el comportamiento con un 1 o 0
def predict_model(model,data):
    data= data.fillna(0.0)
    data.loc[(data.aceleracion == np.inf) | (data.aceleracion == -np.inf),'aceleracion']=0.0
    x_test = data[['velocidad','aceleracion']].values#'p_distancia',
    perro = model.predict(x_test)
    data['cluster'] = perro
    return data
