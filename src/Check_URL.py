#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan  8 09:20:39 2024

@author: eduardo
"""

import csv
import os
import sys
import pandas as pd
import time
import re
import matplotlib.pyplot as plt

os.chdir("../data/")

df_tsv = pd.read_csv("rules-2024.04.04-12.19.tsv", sep='\t', on_bad_lines='warn', index_col=False, encoding="latin1")


# Elimino las filas donde la columna Bot Says contiene valores NULL
df_tsv_sin_null = df_tsv.dropna(subset=['Bot Says'])

# Elimino las filas donde la columna Bot Says contiene valores espacios vacios
df_tsv_sin_nul_sin_vacios = df_tsv_sin_null[df_tsv_sin_null['Bot Says'].apply(lambda x: not x.isspace())]

# Solo dejo las filas donde ACTIVE esta en True
df_tsv_sin_nul_sin_vacios_Active = df_tsv_sin_nul_sin_vacios[df_tsv_sin_nul_sin_vacios['Active'] == True]

# Lista de nombres de columnas relevantes para el analisis
columnas_deseadas = ['Active', 'ID', 'Name','Bot Says']

# Creo un nuevo DataFrame con las columnas deseadas
df_tsv_limpio = df_tsv_sin_nul_sin_vacios_Active[columnas_deseadas].copy()

# Creo una columna nueva destino de los reemplazos
df_tsv_limpio['Bot_Says2'] = df_tsv_limpio['Bot Says']

# limpio las variables de reemploazo que son de la forma ${ algo adentro }
df_tsv_limpio['Bot_Says2'] = df_tsv_limpio['Bot_Says2'].str.replace(r'\${.*?}', '', regex=True)


# # Defino un diccionario de reemplazos
# reemplazos = {'\\t': ' ','¿': ' ','?': ' ', '!': ' ', '¡': ' ','¡': ' ',',': ' ','.': ' ','null': ' ','*': ' ','\\r\\n': ' '}

reemplazos = {'\\t': ' ','\\r\\n': ' '}
# Realizo los reemplazos en una sola línea
for buscar, reemplazar in reemplazos.items():
    df_tsv_limpio['Bot_Says2'] = df_tsv_limpio['Bot_Says2'].str.replace(buscar, reemplazar)


# Defino una expresión regular para encontrar URLs
patron_url = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'


# Función para encontrar URLs y devolverlas en una lista
def encontrar_urls(texto):
    return re.findall(patron_url, str(texto))

# Aplico la función a la columna Bot_Says2 y creo una nueva columna con las listas de URLs
df_tsv_limpio['URLs_encontradas'] = df_tsv_limpio['Bot_Says2'].apply(encontrar_urls)

"""

def verificar_url_activa(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:  # Código de estado 200 indica una respuesta exitosa
            return True
        else:
            return False
    except requests.ConnectionError:  # Capturar error de conexión
        return False

# Ejemplo de uso
url = "https://www.infobae55.com"
if verificar_url_activa(url):
    print("La URL está activa.")
else:
    print("La URL no está activa.")


"""



sys.exit()

