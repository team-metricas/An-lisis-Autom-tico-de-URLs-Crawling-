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
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("check_url.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Desactiva warnings de pandas sobre el modo de encadenamiento
pd.options.mode.chained_assignment = None  # default='warn'

# Función para crear una sesión con reintentos automáticos
def create_session_with_retries(retries=5, backoff_factor=0.3):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# Función para obtener respuesta con reintentos manuales (en caso de que los reintentos automáticos fallen)
def get_with_retries(session, url, headers, max_retries=5):
    for attempt in range(max_retries):
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                response = session.get(url, headers=headers, verify=False, timeout=15)
            return response
        except (requests.exceptions.ChunkedEncodingError, 
                requests.exceptions.ConnectionError, 
                requests.exceptions.ReadTimeout,
                requests.exceptions.RequestException) as e:
            if attempt < max_retries - 1:  # No dormir en el último intento
                sleep_time = 2 * (attempt + 1)  # Backoff exponencial
                logger.warning(f"Intento {attempt+1} fallido para URL {url}. Reintentando en {sleep_time} segundos. Error: {e}")
                time.sleep(sleep_time)
                continue
            else:
                logger.error(f"Error después de {max_retries} intentos para URL {url}: {e}")
                return None

# Función para guardar respaldo periódico
def save_backup(df_procesar, contador, backup_interval=50):
    if contador % backup_interval == 0:
        df_filtrado_parcial = df_procesar[df_procesar['URL_Invalida'].apply(lambda x: isinstance(x, list) and len(x) > 0)]
        backup_filename = f'RuleName_con_urls_malas_backup_{contador}.csv'
        df_filtrado_parcial[['Name', 'URLs_encontradas', 'URL_Invalida']].to_csv(
            backup_filename, sep=';', encoding='iso-8859-1', index=False
        )
        logger.info(f"Backup guardado en {backup_filename} - {contador} filas procesadas")

# Arranco la hora de inicio
inicio = time.time()
logger.info("Iniciando procesamiento de URLs")

try:
    # Cambiar al directorio de datos
    os.chdir("../data/")
    
    # Cargar el archivo TSV
    tsv_file = "rules-2025.05.06-13.03.tsv"
    logger.info(f"Cargando archivo {tsv_file}")
    
    df_tsv = pd.read_csv(tsv_file, sep='\t', on_bad_lines='warn', index_col=False, encoding="latin1")
    logger.info(f"Archivo cargado con {df_tsv.shape[0]} filas")
    
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
    
    # Realizo los reemplazos en una sola línea
    reemplazos = {'\\t': ' ','\\r\\n': ' '}
    for buscar, reemplazar in reemplazos.items():
        df_tsv_limpio['Bot_Says2'] = df_tsv_limpio['Bot_Says2'].str.replace(buscar, reemplazar)
    
    # Defino una expresión regular para encontrar URLs
    patron_url = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    
    # Función para encontrar URLs y devolverlas en una lista
    def encontrar_urls(texto):
        return re.findall(patron_url, str(texto))
    
    # Aplico la función a la columna Bot_Says2 y creo una nueva columna con las listas de URLs
    df_tsv_limpio['URLs_encontradas'] = df_tsv_limpio['Bot_Says2'].apply(encontrar_urls)
    
    # Para producción completa
    df_procesar = df_tsv_limpio.copy()
    # Para pruebas con conjunto reducido
    # df_procesar = df_tsv_limpio.head(100)
    
    # Encabezados de solicitud personalizados
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'es-ES,es;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
    }
    
    # Creación de columna para URLs inválidas
    df_procesar['URL_Invalida'] = None
    
    # Listas para almacenar resultados
    url_total = []
    url_malas = []
    
    # Crear sesión con reintentos
    session = create_session_with_retries(retries=3, backoff_factor=0.5)
    
    # Procesar filas
    numero_filas = df_procesar.shape[0]
    contador = 1
    logger.info(f"Cantidad de filas a procesar: {numero_filas}")
    
    for index, row in df_procesar.iterrows():
        try:
            # Calcular y mostrar progreso
            procesado = str(round((contador / numero_filas) * 100, 1)) + "%"
            contador += 1
            print(f"\t\t\t\tProcesado {procesado}", end="\r")
            
            # Guardar respaldo periódico
            save_backup(df_procesar, contador, backup_interval=50)
            
            # Obtener URLs de la fila actual
            dato1 = row['URLs_encontradas']
            
            resultados = []
            if len(dato1) > 0:
                for url in dato1:
                    # Eliminar punto al final de la URL si existe
                    if url.endswith('.'):
                        url = url[:-1]
                    
                    # Agregar a total de URLs
                    url_total.append(url)
                    
                    # Intentar acceder a la URL
                    esValida = False
                    
                    # Obtener respuesta con reintentos
                    response = get_with_retries(session, url, headers)
                    
                    if response is not None and response.status_code == 200:
                        esValida = True
                        logger.debug(f"URL válida: {url}")
                    else:
                        logger.warning(f"NO pudo entrar a {url}")
                        url_malas.append(url)
                        resultados.append(url)
            
            # Actualizar resultados en el DataFrame
            df_procesar.at[index, 'URL_Invalida'] = resultados
        
        except Exception as e:
            logger.error(f"Error procesando fila {index}: {e}")
            # Continuar con la siguiente fila en caso de error
            continue
    
    # Filtrar solo las filas con URLs inválidas
    df_filtrado = df_procesar[df_procesar['URL_Invalida'].apply(lambda x: isinstance(x, list) and len(x) > 0)]
    
    # Exportar resultados
    logger.info("Exportando resultados finales")
    df_filtrado[['Name', 'URLs_encontradas', 'URL_Invalida']].to_csv('RuleName_con_urls_malas.csv', sep=';', encoding='iso-8859-1', index=False)
    df_filtrado[['Name', 'URLs_encontradas', 'URL_Invalida']].to_csv('../RuleName_con_urls_malas.csv', sep=';', encoding='iso-8859-1', index=False)
    
    # Definir los nombres y valores de los estadísticos a imprimir
    nombre_variable_1 = "Cantidad Total de Urls"
    valor_variable_1 = len(url_total)
    nombre_variable_2 = "Cantidad de Urls Invalidas"
    valor_variable_2 = len(url_malas)
    nombre_variable_2_1 = "Porcentaje de Urls Invalidas"
    valor_variable_2_1 = str(int((len(url_malas) / len(url_total)) * 100 if len(url_total) > 0 else 0)) + "%"
    nombre_variable_3 = "Cantidad de Filas TSV File"
    valor_variable_3 = numero_filas
    
    # Crear una lista de tuplas con los nombres y valores de los estadísticos
    data = [
        (nombre_variable_1, valor_variable_1),
        (nombre_variable_2, valor_variable_2),
        (nombre_variable_2_1, valor_variable_2_1),
        (nombre_variable_3, valor_variable_3)
    ]
    
    # Guardar estadísticas
    nombre_archivo = "../Estadisticos_URLS_TSV.csv"
    with open(nombre_archivo, mode='w', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(['Variable', 'Valor'])  # Escribir la fila de encabezado
        writer.writerows(data)  # Escribir los datos de las variables
    
    # Calcular tiempo transcurrido
    fin = time.time()
    tiempo_transcurrido = fin - inicio
    tiempo_transcurrido_formato = time.strftime("%H:%M:%S", time.gmtime(tiempo_transcurrido))
    
    # Imprimir resumen
    logger.info(f"Resumen del proceso:")
    logger.info(f"Hora de inicio: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(inicio))}")
    logger.info(f"Hora de finalización: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(fin))}")
    logger.info(f"Tiempo transcurrido: {tiempo_transcurrido_formato}")
    logger.info(f"Total de URLs analizadas: {len(url_total)}")
    logger.info(f"URLs inválidas encontradas: {len(url_malas)} ({valor_variable_2_1})")
    
    print("\n\nHora de inicio:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(inicio)))
    print("Hora de finalización:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(fin)))
    print("Tiempo transcurrido:", tiempo_transcurrido_formato)
    print(f"Total de URLs analizadas: {len(url_total)}")
    print(f"URLs inválidas encontradas: {len(url_malas)} ({valor_variable_2_1})")

except Exception as e:
    logger.critical(f"Error fatal en la ejecución del programa: {e}")
    print(f"Error fatal: {e}")
    # Intentar guardar el progreso actual en caso de error crítico
    try:
        if 'df_procesar' in locals():
            df_procesar.to_csv('emergency_backup_error.csv', sep=';', encoding='iso-8859-1', index=False)
            print("Se ha guardado un respaldo de emergencia en 'emergency_backup_error.csv'")
    except:
        print("No se pudo guardar el respaldo de emergencia")