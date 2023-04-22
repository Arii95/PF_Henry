import pandas as pd
from geopy import Point
from shapely.geometry import Point
import geopandas as gpd
import math
from support_api import select_data_by_date,select_data_by_dates ,data_devices,update_aguada



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
        print(data_de.shape)
        data[i]=data_de.iloc[-1][['dataRowData_lat','dataRowData_lng']]
    dtf= pd.DataFrame(data).transpose()
    return dtf

def agua_click(data,vaca,fecha,setle):
    aguadas=update_aguada(setle)
    print(aguadas.shape)
    dtf= gps_aguada(aguadas,data)
    print(dtf.iloc[0,0])
    data_p=filter_area_peri(data, dtf.iloc[0,0], dtf.iloc[0,1],4.0)
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