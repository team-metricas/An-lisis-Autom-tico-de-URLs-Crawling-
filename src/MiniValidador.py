# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 13:22:10 2024

@author: 20171078343
"""

import requests

def verificar_url_activa(url):
    try:
        response = requests.get(url)
        print("response.status_code",response.status_code)
        if response.status_code == 200:  # Código de estado 200 indica una respuesta exitosa
            return True
        else:
            return False
    except requests.ConnectionError:  # Capturar error de conexión
        return False

# Ejemplo de uso
url = "https://directordeobra.agcontrol.gob.ar/"
if verificar_url_activa(url):
    print("La URL está activa.")
else:
    print("La URL no está activa.")
