from memory_profiler import profile
import pandas as pd
import json
import os
import numpy as np
import re

'''

Se tomaron los retweets como un tweet normal
La estrategia que se utilizo fue cargar los datos desde el json sin procesamiento adicional,se realizaron pruebas con libreria multiprocessing para mejorar el rendimiento en carga de datos pero esto se descarto porque no se evidencio una mejora en el tiempo de carga.
Utilizando pandas se transforma la columna content en una lista de los emojis que contiene cada una, luego se transforma en una sola lista de todos los emojis, utilizando un arreglo de numpy se cuentan las repeticiones de cada uno y se retornan en una tupla la cual se ordena y se filtran los 10 mas repetidos,

Esta estrategia es la misma utilizada en el punto 3, se puede idependizar la funcion calculate_tf para ser utiizada en los dos puntos, tambine esta se puede implementar una funcion reduce paralelizada para procesar los datos en menor tiempo

'''

def load_data(file_path):
  with open(file_path) as json_file:
      data = json_file.readlines()
      data = list(map(json.loads, data))
  return data

def calculate_tf(item_list):
  item_list = np.array(item_list)
  term, frequency = np.unique(item_list, return_counts = True)
  return list(zip(term,frequency))

  
def q2_time(file_path: str) -> List[Tuple[str, int]]:
  EMOJI_PATTERN = re.compile(
      "["
      "\U0001F1E0-\U0001F1FF"
      "\U0001F300-\U0001F5FF"
      "\U0001F600-\U0001F64F"
      "\U0001F680-\U0001F6FF" 
      "\U0001F700-\U0001F77F"
      "\U0001F900-\U0001F9FF"
      "\U0001FA00-\U0001FA6F"
      "\U0001FA70-\U0001FAFF"
      "\U00002702-\U000027B0"
      "\U000024C2-\U0001F251"
      "]"
  )
  data = load_data(file_path)
  df = pd.DataFrame(data)
  emoji_list = df.content.str.findall(EMOJI_PATTERN).to_list()
  emoji_list = list(filter(lambda x: len(x) > 0, emoji_list))
  emoji_list = [emoji for row in emoji_list for emoji in row]
  emoji_list = [e for e in emoji_list if e not in ['️','🏻','🏽']]
  tf = calculate_tf(emoji_list)
  tf.sort(key=lambda t: t[1], reverse = True)
  return tf[:10]
  
