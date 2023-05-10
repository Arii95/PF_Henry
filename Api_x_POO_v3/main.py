from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.responses import RedirectResponse
from POO_APIResponse import APIResult
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
    try:
        datos=APIResult(nombre= nombre).respuesta_api()
        if datos[0].shape[0]==0:
                return {'alert':'registros vacios'}
        else:
            return JSONResponse(content= json.loads(datos[0].to_json()))
    except Exception:
        return {'alert':'parametro incorreto o registros vacios'}
    


# Segunda consulta: Informacion propia de una finca en un periodo de tienpo.
@app.get("/informacion_por_un_periodo_por_finca/{nombre}/{fecha_init}/{fecha_fin}")
async def informacion_por_un_periodo_por_finca(nombre : str, fecha_init: str, fecha_fin : str):
    try:
        datos= APIResult(nombre=nombre,fecha_ini=fecha_init,fecha_finsh=fecha_fin).respuesta_api()
        if datos[0].shape[0]==0:
                    return {'alert':'registros vacios'}
        else:
            return JSONResponse(content= json.loads( datos[0].to_json()))
    except Exception:
        return {'alert':'parametro incorreto o registros vacios'}



# Tercera consulta: Toda la informacion de una vaca de un establecimiento
@app.get("/filtro_por_una_vaca_establecimiento/{nombre}/{id}")
async def filtro_por_una_vaca_establecimiento(nombre : str, id : str):
    try:
        datos= APIResult(nombre=nombre,uuid=id).respuesta_api()
        if datos[0].shape[0]==0:
                return {'alert':'registros vacios'}
        else:
            return JSONResponse(content= datos[0].to_dict('records'))
    except Exception:
        return {'alert':'parametro incorreto o registros vacios'}


@app.get("/conducta_vaca/{nombre}/{id}/{fecha}")
async def conducta_vaca(nombre : str, id : str, fecha: str):
    try:
        datos,resumen,diagnostic= APIResult(nombre=nombre,uuid=id,fecha_ini=fecha).respuesta_api()
        if datos.shape[0]==0:
            return {'alert':'registros vacios'}
        else:
            datos={ 'datos':datos.to_dict('records'),
                    'resumen_datos':resumen.to_dict('records'),
                    'diagnostico':diagnostic.to_dict('records')}
            return JSONResponse(content= datos)
    except Exception:
        return {'alert':'parametro incorreto o registros vacios'}


@app.get("/conducta_vaca_periodo/{nombre}/{id}/{fecha_init}/{fecha_fin}")
async  def conducta_vaca_periodo(nombre : str, id : str, fecha_init: str, fecha_fin : str):
    try:
        datos,resumen,diagnostic = APIResult(nombre=nombre,uuid=id,fecha_ini=fecha_init,fecha_finsh=fecha_fin).respuesta_api()
        if datos.shape[0]==0:
            return {'alert':'registros vacios'}
        else:
            datos={ 'datos':datos.to_dict('records'),
                    'resumen_datos':resumen.to_dict('records'),
                    'diagnostico':diagnostic.to_dict('records')}
            return JSONResponse(content= datos)
    except Exception:
        return {'alert':'parametro incorreto o registros vacios'}


