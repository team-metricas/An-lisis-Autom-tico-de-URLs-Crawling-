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
import warnings
import requests

pd.options.mode.chained_assignment = None  # default='warn'

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

#df_procesar = df_tsv_limpio.head(1000)  ## Esto es para debug de un set reducido de filas
df_procesar = df_tsv_limpio.copy()

# Encabezados de solicitud personalizados
headers = {
    'User-Agent': 'Usuario Personalizado',
    'Accept-Language': 'es-ES,es;q=0.9',
}

df_procesar['URL_Invalida'] = None
url_total = []
url_malas = []


numero_filas = df_procesar.shape[0]
contador = 1
print("\n\n\nCantidad de filas:", str(numero_filas))

for index, row in df_procesar.iterrows():

    procesado = str(round((contador / numero_filas)* 100,1))+"%"
    contador = contador +1

    print("\t\t\t\tProcesado ",procesado)
    dato1 = row['URLs_encontradas']
    

    resultados = []
    if len(dato1) > 0:
        for url in dato1:
            if url.endswith('.'):  ## ELimino el Punto al final de la URL
                url = url[:-1]
                
            url_total.append(url)
            max_intentos = 5  # Número máximo de intentos
            intento_actual = 0
            esValida = False
            
            while intento_actual < max_intentos:
                try:
                    # Desactivo la verificación del certificado SSL
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        response = requests.get(url, headers=headers, verify=False)
            
                    
                    if response.status_code == 200:
                        esValida = True
                        #print("Sí pudo entrar a", url)
                        break  # Si la respuesta es 200, devuelvo True
                except requests.exceptions.ConnectionError as e:
                    print("\tError de conexión:", e)
                    print("\tURL inválida:", url)
                    break
                
                intento_actual += 1
                time.sleep(1)  
                     

            if not esValida:
                print("\nNO pudo entrar a ",url)
                url_malas.append(url)
                resultados.append(url)
                
            
            #print(resultados)
    df_procesar.at[index, 'URL_Invalida'] = resultados



df_filtrado = df_procesar[df_procesar['URL_Invalida'].apply(lambda x: isinstance(x, list) and len(x) > 0)]

#Exporto a formato CSV separado con punto y coma asi lo levanta directo un excel
df_filtrado[['Name', 'URLs_encontradas', 'URL_Invalida']].to_csv('RuleName_con_urls_malas.csv',sep=';', encoding='iso-8859-1', index=False)
df_filtrado[['Name', 'URLs_encontradas', 'URL_Invalida']].to_csv('../RuleName_con_urls_malas.csv',sep=';', encoding='iso-8859-1', index=False)


# Defino los nombres y valores de los estadisticos a imprimir
nombre_variable_1 = "Cantidad Total de Urls"
valor_variable_1 = len(url_total)
nombre_variable_2 = "Cantidad de Urls Invalidas"
valor_variable_2 = len(url_malas)
nombre_variable_2_1 = "Porcentaje de Urls Invalidas"
valor_variable_2_1 = str(int((len(url_malas) / len(url_total)) * 100 )) + "%"
nombre_variable_3 = "Cantidad de Filas TSV File"
valor_variable_3 = numero_filas

# Creo una lista de tuplas con los nombres y valores de los estadisticos de interes
data = [
    (nombre_variable_1, valor_variable_1),
    (nombre_variable_2, valor_variable_2),
    (nombre_variable_2_1, valor_variable_2_1),
    (nombre_variable_3, valor_variable_3)
]

nombre_archivo = "../Estadisticos_URLS_TSV.csv"

# Escribir los datos en el archivo CSV
with open(nombre_archivo, mode='w', newline='') as file:
    writer = csv.writer(file,delimiter=';')
    writer.writerow(['Variable', 'Valor'])  # Escribir la fila de encabezado
    writer.writerows(data)  # Escribir los datos de las variables


# Calculo final de elapsed time del programa
fin = time.time()
tiempo_transcurrido = fin - inicio
tiempo_transcurrido_formato = time.strftime("%H:%M:%S", time.gmtime(tiempo_transcurrido))


print("\n\nHora de inicio:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(inicio)))
print("Hora de finalización:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(fin)))
print("Tiempo transcurrido:", tiempo_transcurrido_formato)

