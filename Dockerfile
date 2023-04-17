FROM tiangolo/uvicorn-gunicorn-fastapi

RUN pip3 install fastapi

RUN pip3 install pandas

RUN pip3 install pymongo

RUN pip3 install matplotlib

RUN pip3 install plotly

RUN pip3 install geopandas 

RUN pip3 install json

RUN pip3 install geopy

RUN pip3 install typing

COPY ./app .