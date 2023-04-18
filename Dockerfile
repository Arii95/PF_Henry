FROM tiangolo/uvicorn-gunicorn-fastapi

RUN pip install fastapi

RUN pip install pandas

RUN pip install pymongo

RUN pip install matplotlib

RUN pip install plotly

RUN pip install geopandas 

RUN pip install json

RUN pip install geopy

RUN pip install typing

COPY ./app .