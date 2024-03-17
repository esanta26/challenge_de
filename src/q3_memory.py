from memory_profiler import profile
import pandas as pd
import json
import os
import numpy as np
import re

'''

Se tomaron los retweets como un tweet normal
La estrategia que se utilizo fue cargar los datos desde el json en una lista, solo se carga el campo mentionedUsers.
inicialmente se filtran los campos nulos que corresponden a tweets que no tienen menciones, posterior se transforma la lista de listas de usuarios mencionados en una sola lista con los nombres de usuarios contenidos en estas y se utiliza la misma estrategia del punto 2 realizando conteo de repeticiones con un arreglo de numpy

'''

@profile
def load_data(file_path):
  with open(file_path) as json_file:
      data = json_file.readlines()
      data = list(map(lambda x: json.loads(x).get("mentionedUsers"), data))
  return data

@profile
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
  data = load_data(file_path)
  referenced_users = list(filter(lambda x: x is not None, data))
  referenced_users = [user.get("username") for row in referenced_users for user in row]
  tf = calculate_tf(referenced_users)
  tf.sort(key=lambda t: t[1], reverse = True)
  return tf[:10]

