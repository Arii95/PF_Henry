import pandas as pd 
from funciones_app import data_devices,dataframe_interview_vaca,select_data_by_date,update_aguada,select_data_by_dates
from conect_datarows import setle_clean,df_gps,add_dormida_column,separador_por_dia,diagnostico_devices
from ml_streamlit import predict_model
from geopy import Point
from shapely.geometry import Point
import geopandas as gpd
import math

def filter_area_peri(data,latitud,longitud,metro):
    gdf= gpd.GeoDataFrame(data,crs='EPSG:4326',geometry=gpd.points_from_xy(data.dataRowData_lng,data.dataRowData_lat))
    setle_lat=latitud
    setle_lng=longitud
    punto_referencia= Point(setle_lng,setle_lat)	
    per_kilo= math.sqrt(metro)*0.01
    circulo= punto_referencia.buffer(per_kilo/111.32) # valor 1 grado aprox en kilometro en el ecuador 
    on_perimetro= gdf[gdf.geometry.within(circulo)]
    agua = update_aguada(on_perimetro)
    on_perimetro = on_perimetro.drop(on_perimetro[on_perimetro['UUID'].isin(agua.deviceMACAddress.unique())].index)
    return on_perimetro

def gps_aguada(aguadas,df):
    movi_agu= df[df.UUID.isin(aguadas.deviceMACAddress.unique())]
    data={}
    for i in aguadas.deviceMACAddress:
        data_de = data_devices(movi_agu,i)
        data[i]=data_de.iloc[-1][['dataRowData_lat','dataRowData_lng']]
    dtf= pd.DataFrame(data).transpose()
    return dtf

def agua_click(data,vaca,fecha,setle):
    aguadas=update_aguada(setle)
    dtf= gps_aguada(aguadas,data)
    data_p=filter_area_peri(data, dtf.iloc[0, 0], dtf.iloc[0, 1],4.0)
    day_p=select_data_by_date(data_p,fecha)
    p=data_devices(day_p,vaca)
    return p


def agua_clicks(data,vaca,fecha,fecha2,setle):
        aguadas=update_aguada(setle)
        dtf= gps_aguada(aguadas,data)
        data_p=filter_area_peri(data, dtf.iloc[0,0], dtf.iloc[0,1],4.0)
        day_p=select_data_by_dates(data_p,fecha,fecha2)
        p=data_devices(day_p,vaca)
        return p

def result_select(data_values,data):
    select=data_values.point_ini.dt.strftime('%H:%M').isin(data.createdAt.dt.strftime('%H:%M').values)
    data_values.loc[select,'agua']=1
    data_values.agua= data_values.agua.fillna(0)
    return data_values



def conducta_vaca_periodo(df, df_para_aguada,uuid, nombre, fecha_init: str, fecha_fin : str):
    data_finca= setle_clean(nombre)
    df_gp = dataframe_interview_vaca(df)
    df_gp = predict_model(df_gp)
    df_gp = add_dormida_column(df_gp, 1, 20, 6)
    d = agua_clicks(df_para_aguada,uuid,fecha_init,fecha_fin,str(data_finca._id.values[0]))
    df_gp = result_select(df_gp,d)
    resumen = separador_por_dia(df_gp)
    diagnostico = diagnostico_devices(resumen)
    df_gp = df_gp.drop(columns=['fecha'])
    return df_gp,resumen,diagnostico


