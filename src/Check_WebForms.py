# -*- coding: utf-8 -*-
"""
Created on Wed May 15 10:30:25 2025

@author: eduardo
"""

import csv
import os
import sys
import pandas as pd
import time
import re
import logging
import glob  # Añadido para buscar archivos por patrón

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("find_google_forms.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Desactiva warnings de pandas sobre el modo de encadenamiento
pd.options.mode.chained_assignment = None  # default='warn'

# Función para guardar respaldo periódico
def save_backup(df_procesar, contador, backup_interval=50):
    if contador % backup_interval == 0:
        df_filtrado_parcial = df_procesar[df_procesar['GoogleForms_Encontrados'].apply(lambda x: isinstance(x, list) and len(x) > 0)]
        backup_filename = f'RuleName_con_google_forms_backup_{contador}.csv'
        df_filtrado_parcial[['Name', 'GoogleForms_Encontrados']].to_csv(
            backup_filename, sep=';', encoding='iso-8859-1', index=False
        )
        logger.info(f"Backup guardado en {backup_filename} - {contador} filas procesadas")

# Función para eliminar todos los archivos de respaldo
def delete_backups():
    try:
        # Buscar todos los archivos de respaldo por patrón
        backup_files = glob.glob('RuleName_con_google_forms_backup_*.csv')
        
        # Eliminar cada archivo
        for file in backup_files:
            os.remove(file)
            logger.info(f"Archivo de respaldo eliminado: {file}")
        
        logger.info(f"Total de {len(backup_files)} archivos de respaldo eliminados")
    except Exception as e:
        logger.error(f"Error al eliminar archivos de respaldo: {e}")

# Arranco la hora de inicio
inicio = time.time()
logger.info("Iniciando búsqueda de Google Forms")

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
    
    # Defino expresiones regulares para encontrar Google Forms
    # Patrón para URLs con https:// y sin él
    patron_google_forms = r'https?://docs\.google\.com/forms[/\w\d\?=&%\.\-]*'
    patron_google_forms_sin_protocolo = r'(?<!\w)docs\.google\.com/forms[/\w\d\?=&%\.\-]*'
    
    # Función para encontrar Google Forms y devolverlos en una lista
    def encontrar_google_forms(texto):
        forms_con_protocolo = re.findall(patron_google_forms, str(texto))
        forms_sin_protocolo = re.findall(patron_google_forms_sin_protocolo, str(texto))
        
        # Convertir los forms sin protocolo a formato completo
        forms_completos = ["https://" + form for form in forms_sin_protocolo]
        
        # Combinar resultados y eliminar duplicados
        todos_los_forms = list(set(forms_con_protocolo + forms_completos))
        return todos_los_forms
    
    # Aplico la función a la columna Bot_Says2 y creo una nueva columna con las listas de Google Forms
    df_tsv_limpio['GoogleForms_Encontrados'] = df_tsv_limpio['Bot_Says2'].apply(encontrar_google_forms)
    
    # Para producción completa
    df_procesar = df_tsv_limpio.copy()
    # Para pruebas con conjunto reducido
    # df_procesar = df_tsv_limpio.head(100)
    
    # Listas para almacenar resultados
    all_google_forms = []
    
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
            
            # Obtener Google Forms de la fila actual
            forms_encontrados = row['GoogleForms_Encontrados']
            
            # Añadir a la lista general de Google Forms
            if forms_encontrados and len(forms_encontrados) > 0:
                for form in forms_encontrados:
                    all_google_forms.append(form)
        
        except Exception as e:
            logger.error(f"Error procesando fila {index}: {e}")
            # Continuar con la siguiente fila en caso de error
            continue
    
    # Filtrar solo las filas con Google Forms
    df_filtrado = df_procesar[df_procesar['GoogleForms_Encontrados'].apply(lambda x: isinstance(x, list) and len(x) > 0)]
    
    # Exportar resultados
    logger.info("Exportando resultados finales")
    df_filtrado[['Name', 'GoogleForms_Encontrados']].to_csv('RuleName_con_google_forms.csv', sep=';', encoding='iso-8859-1', index=False)
    df_filtrado[['Name', 'GoogleForms_Encontrados']].to_csv('../RuleName_con_google_forms.csv', sep=';', encoding='iso-8859-1', index=False)
    
    # Crear lista de Google Forms únicos
    unique_google_forms = list(set(all_google_forms))
    
    # Exportar lista de Google Forms únicos
    with open('../Lista_GoogleForms_Unicos.csv', 'w', newline='', encoding='iso-8859-1') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(['Google Form URL'])
        for form in unique_google_forms:
            writer.writerow([form])
    
    # Definir los nombres y valores de los estadísticos a imprimir
    nombre_variable_1 = "Cantidad Total de Google Forms (con duplicados)"
    valor_variable_1 = len(all_google_forms)
    nombre_variable_2 = "Cantidad de Google Forms Únicos"
    valor_variable_2 = len(unique_google_forms)
    nombre_variable_3 = "Cantidad de Filas TSV con Google Forms"
    valor_variable_3 = df_filtrado.shape[0]
    nombre_variable_4 = "Cantidad de Filas TSV File"
    valor_variable_4 = numero_filas
    nombre_variable_5 = "Porcentaje de Filas con Google Forms"
    valor_variable_5 = str(round((df_filtrado.shape[0] / numero_filas) * 100, 2)) + "%"
    
    # Crear una lista de tuplas con los nombres y valores de los estadísticos
    data = [
        (nombre_variable_1, valor_variable_1),
        (nombre_variable_2, valor_variable_2),
        (nombre_variable_3, valor_variable_3),
        (nombre_variable_4, valor_variable_4),
        (nombre_variable_5, valor_variable_5)
    ]
    
    # Guardar estadísticas
    nombre_archivo = "../Estadisticos_GoogleForms_TSV.csv"
    with open(nombre_archivo, mode='w', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(['Variable', 'Valor'])  # Escribir la fila de encabezado
        writer.writerows(data)  # Escribir los datos de las variables
    
    # Eliminar todos los archivos de respaldo
    logger.info("Eliminando archivos de respaldo...")
    delete_backups()
    
    # Calcular tiempo transcurrido
    fin = time.time()
    tiempo_transcurrido = fin - inicio
    tiempo_transcurrido_formato = time.strftime("%H:%M:%S", time.gmtime(tiempo_transcurrido))
    
    # Imprimir resumen
    logger.info(f"Resumen del proceso:")
    logger.info(f"Hora de inicio: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(inicio))}")
    logger.info(f"Hora de finalización: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(fin))}")
    logger.info(f"Tiempo transcurrido: {tiempo_transcurrido_formato}")
    logger.info(f"Total de Google Forms encontrados: {len(all_google_forms)}")
    logger.info(f"Google Forms únicos: {len(unique_google_forms)}")
    logger.info(f"Filas con Google Forms: {df_filtrado.shape[0]} de {numero_filas} ({valor_variable_5})")
    
    print("\n\nHora de inicio:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(inicio)))
    print("Hora de finalización:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(fin)))
    print("Tiempo transcurrido:", tiempo_transcurrido_formato)
    print(f"Total de Google Forms encontrados: {len(all_google_forms)}")
    print(f"Google Forms únicos: {len(unique_google_forms)}")
    print(f"Filas con Google Forms: {df_filtrado.shape[0]} de {numero_filas} ({valor_variable_5})")

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