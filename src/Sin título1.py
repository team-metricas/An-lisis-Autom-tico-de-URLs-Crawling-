import requests

# URL a la que deseas hacer la solicitud
url = 'https://www.cancilleria.gob.ar/es/servicios/servicios/apostilla-legalizacion-con-validez-internacional-tad'

# Encabezados de solicitud personalizados
headers = {
    'User-Agent': 'Mi Agente de Usuario Personalizado',
    'Accept-Language': 'es-ES,es;q=0.9',
    # Otros encabezados personalizados según sea necesario
}

# Datos de autenticación si es necesario
auth = ('usuario', 'contraseña')  # Reemplaza 'usuario' y 'contraseña' con tus credenciales

try:
    # Realizar la solicitud con los encabezados y autenticación
    response = requests.get(url, headers=headers)

    # Verificar el código de estado de la respuesta
    if response.status_code == 200:
        print('La solicitud fue exitosa')
        
        #print(response.text)  # Imprimir el contenido de la respuesta si es necesario
    else:
        print(f'Error: La solicitud devolvió el código de estado {response.status_code}')

except requests.exceptions.RequestException as e:
    print('Error al realizar la solicitud:', e)
