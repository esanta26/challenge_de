from datetime import datetime
import pandas as pd
import json
import os
from memory_profiler import profile

'''

Se tomaron los retweets como un tweet normal
La estrategia que se utilizo fue cargar los datos desde el json con solo las columnas de fecha y nombre de usuario, se adiciono un procesamiento en la carga del usuario para extraer el nombre

'''
@profile
def load_data(file_path):
  with open(file_path) as json_file:
      data = json_file.readlines()
      data = list(map(json.loads, data))
      data = [{k:d[k] if k != 'user' else d[k].get('username') for k in ["date","user"]} for d in data]
  return data


@profile
def q1_memory(file_path: str):
  data = load_data()
  df = pd.DataFrame(data)
  df['date'] = pd.to_datetime(df['date']).dt.date
  df['user'] = df['user'].astype("string")
  dates = df.groupby(['date']).agg(count=("date", 'count')).reset_index().sort_values(by=['count'], ascending=False).head(10)['date'].to_list()
  df = df[df['date'].isin(dates)]
  df = pd.crosstab(df['date'], df['user'])
  return list(df.idxmax(axis=1).to_dict().items())
