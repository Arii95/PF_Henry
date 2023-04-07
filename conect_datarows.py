import pymongo
import pandas as pd

data_mongo= pymongo.MongoClient('localhost:27017')

db= data_mongo['test']
rows=db['datarows']

drow1= pd.json_normalize(list(rows.find()[:150000]),sep='_')
drow2= pd.json_normalize(list(rows.find()[150000:300000]),sep='_')
drow3= pd.json_normalize(list(rows.find()[300000:450000]),sep='_')
drow4= pd.json_normalize(list(rows.find()[450000:600000]),sep='_')
drow5= pd.json_normalize(list(rows.find()[600000:750000]),sep='_')
drow6= pd.json_normalize(list(rows.find()[750000:900000]),sep='_')
drow7= pd.json_normalize(list(rows.find()[900000:1050000]),sep='_')
drow8= pd.json_normalize(list(rows.find()[1050000:1200000]),sep='_')
drow9= pd.json_normalize(list(rows.find()[1200000:]),sep='_')

df_row=pd.concat([drow1,drow2,drow3,drow4,drow5,drow6,drow7,drow8,drow9],axis=0)
df_row.shape

df_row._id=df_row._id.astype(str)