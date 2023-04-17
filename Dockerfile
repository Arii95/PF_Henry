FROM tiangolo/uvicorn-gunicorn-fastapi

RUN pip3 install fastapi

RUN pip3 install pandas

RUN pip3 install pymongo

RUN pip3 matplotlib

RUN pip3 install plotly

RUN pip3 install numpy

RUN pip3 install seaborn

RUN pip3 install geopandas 

RUN pip3 install datetime

RUN pip3 installstring

RUN pip3 install random

RUN pip3 install folium

RUN pip3 install math

RUN pip3 install json

RUN pip3 install sklearn

COPY ./app .