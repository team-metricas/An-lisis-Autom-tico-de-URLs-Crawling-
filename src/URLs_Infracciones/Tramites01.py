import pandas as pd

# Cargar el archivo CSV con separador punto y coma
df = pd.read_csv("enlaces_invalidos_infracciones_20250528_173336.csv", sep=';')

# Filtro 1: URL_PAGINA_ORIGEN comienza con el prefijo dado
filtro_tramites = df['URL_PAGINA_ORIGEN'].astype(str).str.startswith("https://buenosaires.gob.ar/tramites/")

# Filtro 2: URL_DESTINO_INVALIDA NO contiene '@'
filtro_sin_arroba = ~df['URL_DESTINO_INVALIDA'].astype(str).str.contains('@')

# Aplicar ambos filtros
df_filtrado = df[filtro_tramites & filtro_sin_arroba]

# Exportar el resultado con separador punto y coma
df_filtrado.to_csv("tramites_sin_arroba.csv", index=False, sep=';')
