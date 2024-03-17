from datetime import datetime
import pandas as pd
import json
import os
from memory_profiler import profile

'''

Se tomaron los retweets como un tweet normal
La estrategia que se utilizo fue cargar los datos desde el json sin realizar procesamiento, solo los datos de la fecha y usuario, adiciona
Inicialmente se filtran las fechas con mas tweets, luego se hace una tabla cruzada donde cada columna es usuario, cada fila es una fecha y el valor es el conteo de tweet para ese usuario en esa fecha, 
se utiliza el metodo idxmax para encontrar la columna maxima en cada fecha

'''
@profile
def load_data(file_path):
  with open(file_path) as json_file:
      data = json_file.readlines()
      data = list(map(json.loads, data))
  return data

@profile
def q1_time(file_path: str):
  data = load_data(file_path)
  df = pd.DataFrame(data)
  df['date'] = pd.to_datetime(df['date']).dt.date
  df['user'] = df.apply(lambda row: row.user.get('username'), axis=1)
  df['user'] = df['user'].astype("category")
  dates = df.groupby(['date']).agg(count=("date", 'count')).reset_index().sort_values(by=['count'], ascending=False).head(10)['date'].to_list()
  df = df[df['date'].isin(dates)]
  df = pd.crosstab(df['date'], df['user'])
  return list(df.idxmax(axis=1).to_dict().items())

