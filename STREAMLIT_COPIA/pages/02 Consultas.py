import streamlit as st
import pandas as pd
from suport_st import grafic_map,mapbox_access_token
import plotly.graph_objects as go
import plotly.express as px
from funciones_app import filter_time_day,dataframe_interview_vaca,data_devices,week_data_filter, filter_area_perimetro
from conect_datarows import setle_clean,selec_setle,obtener_fecha_inicio_fin, df_gps,setle_list
import datetime
import requests
import json




st.title('Consulta de status')

