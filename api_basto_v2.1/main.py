from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.responses import RedirectResponse
import pandas as pd
from filter_and_conection import filter_area_perimetro,df_gps,select_data_by_dates
from process_and_transform_data import data_interview
from process_and_transform_data import process_and_transform_data, resumen_and_diagnostic,process_and_transform_datas
import json
#Creo una instancia de FastAPI
app = FastAPI()

#---- redireccion al docs de fastapi--------
@app.get('/')
async def root():
    return RedirectResponse(url='/docs/')



#---------- Queries-----
# Primer consulta: Informacion propia de una finca.
@app.get("/informacion_por_finca/{nombre}")
async def informacion_por_finca(nombre: str):
    merge_data = data_interview(nombre)
    merge_data.point_ini= merge_data.point_ini.astype(str)
    merge_data.point_next= merge_data.point_next.astype(str)
    return JSONResponse(content= json.loads(merge_data.to_json()))


# Segunda consulta: Informacion propia de una finca en un periodo de tienpo.
@app.get("/informacion_por_un_periodo_por_finca/{nombre}/{fecha_init}/{fecha_fin}")
async def informacion_por_un_periodo_por_finca(nombre : str, fecha_init: str, fecha_fin : str):
    df_gp = filter_area_perimetro(df_gps,nombre)
    df_gp = select_data_by_dates(df_gp,fecha_init,fecha_fin)
    df_gp = data_interview(nombre,df_gp)
    df_gp.point_ini= df_gp.point_ini.astype(str)
    df_gp.point_next= df_gp.point_next.astype(str)
    return JSONResponse(content= json.loads(df_gp.to_json()))



# Tercera consulta: Toda la informacion de una vaca de un establecimiento
@app.get("/filtro_por_una_vaca_establecimiento/{nombre}/{id}")
async def filtro_por_una_vaca_establecimiento(nombre : str, id : str):
    data_finca = data_interview(nombre)
    df_gps = data_finca[data_finca.UUID==id]
    df_gps.point_ini= df_gps.point_ini.astype(str)
    df_gps.point_next= df_gps.point_next.astype(str)
    return JSONResponse(content= json.loads( df_gps.to_json()))


@app.get("/conducta_vaca/{nombre}/{id}/{fecha}")
async def conducta_vaca(nombre : str, id : str, fecha: str):
    df_gp= process_and_transform_data(df_gps, nombre, id ,fecha)
    resumen,diagnostico= resumen_and_diagnostic(df_gp)
    resumen.index= resumen.index.astype(str)
    diagnostico.fecha= diagnostico.fecha.astype(str)
    df_gp.point_ini= df_gp.point_ini.astype(str)
    df_gp.point_next= df_gp.point_next.astype(str)
    df_gp=df_gp.drop(columns=['fecha'])
    datos={'datos':df_gp.to_dict('records'),'resumen_datos':resumen.to_dict('records'),'diagnostico':diagnostico.to_dict('records')}
    return JSONResponse(content= datos)


@app.get("/conducta_vaca_periodo/{nombre}/{id}/{fecha_init}/{fecha_fin}")
async  def conducta_vaca_periodo(nombre : str, id : str, fecha_init: str, fecha_fin : str):
    df_gp= process_and_transform_datas(df_gps, nombre, id ,fecha_init, fecha_fin)
    resumen , diagnostico= resumen_and_diagnostic(df_gp)
    resumen.index= resumen.index.astype(str)
    diagnostico.fecha= diagnostico.fecha.astype(str)
    df_gp.point_ini= df_gp.point_ini.astype(str)
    df_gp.point_next= df_gp.point_next.astype(str)
    df_gp=df_gp.drop(columns=['fecha'])
    datos={'datos':df_gp.to_dict('records'),'resumen_datos':resumen.to_dict('records'),'diagnostico':diagnostico.to_dict('records')}
    return JSONResponse(content= datos)


