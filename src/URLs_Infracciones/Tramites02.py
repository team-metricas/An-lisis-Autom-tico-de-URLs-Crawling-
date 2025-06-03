import pandas as pd
import requests
from bs4 import BeautifulSoup

# Cargar el archivo original
df = pd.read_csv("tramites_sin_arroba.csv", sep=';')

# Crear nueva columna de resultados
df["DESTINO_ACTIVO"] = "No se pudo verificar"

# Procesar cada fila
for index, row in df.iterrows():
    origen = row['URL_PAGINA_ORIGEN']
    destino = row['URL_DESTINO_INVALIDA']
    
    # Ignorar si contiene "ash.buenosaires.gob.ar"
    if "ash.buenosaires.gob.ar" in str(destino):
        df.at[index, "DESTINO_ACTIVO"] = "Ignorado (ash)"
        continue

    try:
        # Obtener el contenido de la página origen
        response = requests.get(origen, timeout=10)
        if response.status_code == 200:
            if destino in response.text:
                # El enlace existe en la página, ahora verificar si funciona
                df.at[index, "DESTINO_ACTIVO"] = "Enlace existe en página"
                try:
                    destino_response = requests.get(destino, timeout=10)
                    if destino_response.status_code == 200:
                        df.at[index, "DESTINO_ACTIVO"] = "Enlace funciona"
                    else:
                        df.at[index, "DESTINO_ACTIVO"] = f"Enlace roto - Error {destino_response.status_code}"
                except Exception as e:
                    df.at[index, "DESTINO_ACTIVO"] = f"Enlace roto - Error: {str(e)}"
            else:
                # El enlace NO existe en la página (esto es normal, no es un error)
                df.at[index, "DESTINO_ACTIVO"] = "Enlace no existe en página"
        else:
            df.at[index, "DESTINO_ACTIVO"] = f"Error al acceder página origen - {response.status_code}"
    except Exception as e:
        df.at[index, "DESTINO_ACTIVO"] = f"Error al acceder página origen - {str(e)}"

# Guardar todos los resultados
df.to_csv("verificacion_enlaces.csv", sep=';', index=False)

# Filtrar solo los enlaces que están rotos (no los que no existen en la página)
df_rotos = df[df["DESTINO_ACTIVO"].str.startswith("Enlace roto")]

# Guardar solo los enlaces que realmente están rotos
df_rotos.to_csv("enlaces_rotos.csv", sep=';', index=False)

print(f"Resultados guardados:")
print(f"- Todos los resultados: verificacion_enlaces.csv")
print(f"- Solo enlaces rotos: enlaces_rotos.csv ({len(df_rotos)} enlaces)")