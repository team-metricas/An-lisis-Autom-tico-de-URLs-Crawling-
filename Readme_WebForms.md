# Check_WebForms.py

## Descripción
`Check_WebForms.py` es una herramienta de análisis diseñada para buscar y extraer URLs de Google Forms presentes en un archivo TSV de reglas de bot. El programa analiza el texto de las respuestas del bot, identifica URLs de Google Forms (tanto con "https://" como sin él), y genera reportes detallados sobre los formularios encontrados.

## Características
- Búsqueda de URLs de Google Forms usando expresiones regulares optimizadas
- Procesamiento robusto de archivos TSV con manejo de errores
- Respaldos automáticos durante la ejecución para prevenir pérdida de datos
- Limpieza de archivos temporales al finalizar
- Generación de reportes completos y estadísticas
- Sistema de logging detallado

## Requisitos
- Python 3.6+
- Pandas
- Otras dependencias estándar de Python (re, csv, os, time, logging, glob)

## Instalación
1. Clone este repositorio:
   ```
   git clone git@github.com:team-metricas/An-lisis-Autom-tico-de-URLs-Crawling-.git
   cd An-lisis-Autom-tico-de-URLs-Crawling/src
   ```

2. Instale las dependencias:
   ```
   pip install pandas
   ```

## Uso
1. Coloque su archivo TSV en el directorio `../data/` relativo al script.
2. Ejecute el script:
   ```
   python Check_WebForms.py
   ```
3. Los resultados se guardarán en el directorio raíz y en `../data/`.

## Archivos generados
- `RuleName_con_google_forms.csv`: Filas del TSV original que contienen Google Forms.
- `Lista_GoogleForms_Unicos.csv`: Lista de todas las URLs únicas de Google Forms encontradas.
- `Estadisticos_GoogleForms_TSV.csv`: Estadísticas del análisis.
- `find_google_forms.log`: Registro detallado de la ejecución.

## Estructura del archivo TSV esperado
El script espera un archivo TSV con la siguiente estructura mínima:
- Una columna `Active` (booleana)
- Una columna `ID` (identificador)
- Una columna `Name` (nombre de la regla)
- Una columna `Bot Says` (texto de respuesta del bot)

## Funcionamiento interno
1. Carga el archivo TSV y filtra filas no relevantes (inactivas, vacías)
2. Limpia el texto de la respuesta del bot, eliminando variables y caracteres especiales
3. Aplica expresiones regulares para identificar URLs de Google Forms
4. Genera un listado completo de todas las URLs y filas asociadas
5. Crea respaldos periódicos durante el procesamiento
6. Al finalizar, genera reportes y elimina archivos temporales

## Personalización
Para modificar el comportamiento del script, puede ajustar:
- La ruta del archivo TSV en `tsv_file = "rules-2025.05.06-13.03.tsv"`
- La frecuencia de respaldos en `backup_interval=50`
- Los patrones de búsqueda en `patron_google_forms` y `patron_google_forms_sin_protocolo`



### Contribución  
¡Las contribuciones son bienvenidas! Si deseas contribuir a este proyecto, sigue estos pasos:

1. Haz un fork del proyecto.
2. Crea una nueva rama para tu funcionalidad o corrección de errores (`git checkout -b feature/nueva-funcionalidad`).
3. Realiza tus cambios y haz commits con mensajes claros y concisos (`git commit -m 'Descripción de los cambios'`).
4. Sube tus cambios a tu repositorio (`git push origin feature/nueva-funcionalidad`).
5. Abre un Pull Request en el repositorio original y describe los cambios que has realizado.

Por favor, asegúrate de que tu código sigue los estándares de estilo del proyecto y que todas las pruebas pasan correctamente antes de enviar tu Pull Request.


### Licencia 

Este proyecto está bajo la Licencia MIT.

MIT License

Derechos de autor (c) [2025] [Eduardo Damian Veralli]

Se concede permiso por la presente, sin cargo, a cualquier persona que obtenga una copia
de este software y los archivos de documentación asociados (el "Software"), para tratar
en el Software sin restricciones, incluidos, entre otros, los derechos
para usar, copiar, modificar, fusionar, publicar, distribuir, sublicenciar y/o vender
copias del Software, y para permitir a las personas a quienes se les proporcione el Software
hacerlo, sujeto a las siguientes condiciones:

El aviso de copyright anterior y este aviso de permiso se incluirán en todos
copias o partes sustanciales del Software.

EL SOFTWARE SE PROPORCIONA "TAL CUAL", SIN GARANTÍA DE NINGÚN TIPO, EXPRESA O
IMPLÍCITA, INCLUYENDO PERO NO LIMITADO A LAS GARANTÍAS DE COMERCIABILIDAD,
IDONEIDAD PARA UN PROPÓSITO PARTICULAR Y NO INFRACCIÓN. EN NINGÚN CASO LOS AUTORES O
LOS TITULARES DEL COPYRIGHT SERÁN RESPONSABLES POR CUALQUIER RECLAMACIÓN, DAÑO U OTRA RESPONSABILIDAD,
YA SEA EN UNA ACCIÓN DE CONTRATO, AGRAVIO O DE OTRO MODO, QUE SURJA DE, FUERA DE O EN
CONEXIÓN CON EL SOFTWARE O EL USO U OTROS TRATOS EN EL SOFTWARE.


### Contacto 

Para preguntas, sugerencias o comentarios, puedes contactar a:

Eduardo Damián Veralli - [@edveralli](https://x.com/EdVeralli) - edveralli@gmail.com

Enlace del Proyecto: [https://github.com/team-metricas/An-lisis-Autom-tico-de-URLs-Crawling-](https://github.com/team-metricas/An-lisis-Autom-tico-de-URLs-Crawling-)


