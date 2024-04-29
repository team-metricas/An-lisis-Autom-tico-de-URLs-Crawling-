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
import requests

# Arranco la hora de inicio
inicio = time.time()


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


df_primeras_10_filas = df_tsv_limpio.head(50)

# Encabezados de solicitud personalizados
headers = {
    'User-Agent': 'Usuario Personalizado',
    'Accept-Language': 'es-ES,es;q=0.9',
}

# # Función para verificar si una URL es válida
# def verificar_url(url):
#     try:
#         response = requests.get(url, headers=headers)
#         print("\n\n\nTesteo ",url)
#         return response.status_code == 20  # Retorna True si el código de estado es 200 (OK)
#     except Exception as e:
#         print(f"\n\n\nError al verificar URL {url}: {e}")
#         #sys.exit()
#         return False

# # Función para verificar URLs y devolver una lista de resultados
# def verificar_urls(urls):
#     resultados = []
#     for url in urls:
#         resultados.append(verificar_url(url))
#     return resultados

df_primeras_10_filas['URL_Invalida'] = None


#print("sigoo")

for index, row in df_primeras_10_filas.iterrows():
    
    dato1 = row['URLs_encontradas']
    
    #dato2 = row['Columna2']
    #print(dato1)
    print("\n\n\nTesteo ",dato1)
    resultados = []
    if len(dato1) > 0:
        for url in dato1:
            #resultados.append(verificar_url(url))
            #resultados.append(dato1)
            #print("Fila completa ",dato1)
            try:
                response = requests.get(url, headers=headers)
                valor =  response.status_code == 200  # Retorna True si el código de estado es 200 (OK)
                if valor:
                    print("SI pudo entrar a ",url)
            except Exception as e:
                print("\n\n\nNO pudo entrar a ",url)
                resultados.append(url)
            
            #print(resultados)
    df_primeras_10_filas.at[index, 'URL_Invalida'] = resultados
    #sys.exit()


    # Realiza el cálculo utilizando los datos de las columnas
    # Ejemplo de cálculo: suma de los datos de las columnas
    
    #resultado_fila = dato1 + dato2sultado_fila = calcular_resultado(row)
    
    # Actualiza la columna 'URLs_encontradas' con el resultado
    #df_primeras_10_filas.at[index, 'URLs_encontradas'] = resultado_fila



# # Aplico la función a la columna 'URLs_encontradas' y crea una nueva columna 'URL_valida'
# df_primeras_10_filas['URL_valida'] = df_primeras_10_filas['URLs_encontradas'].apply(verificar_urls)

#filas_con_false = df_primeras_10_filas[df_primeras_10_filas['URL_Invalida'].astype(str).str.contains('False')]
df_filtrado = df_primeras_10_filas[df_primeras_10_filas['URL_Invalida'].apply(lambda x: isinstance(x, list) and len(x) > 0)]

#Exporto a formato CSV separado con punto y coma asi lo levanta directo un excell
df_filtrado[['Name', 'URLs_encontradas', 'URL_Invalida']].to_csv('RuleName_con_urls_malas.csv',sep=';', index=False)
df_filtrado[['Name', 'URLs_encontradas', 'URL_Invalida']].to_csv('../RuleName_con_urls_malas.csv',sep=';', index=False)

# Calculo final de elapsed time del programa
fin = time.time()
tiempo_transcurrido = fin - inicio
tiempo_transcurrido_formato = time.strftime("%H:%M:%S", time.gmtime(tiempo_transcurrido))


print("\n\nHora de inicio:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(inicio)))
print("Hora de finalización:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(fin)))
print("Tiempo transcurrido:", tiempo_transcurrido_formato)

