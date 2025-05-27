# -*- coding: utf-8 -*-
"""
Verificador de enlaces para infracciones de Buenos Aires
URL base: https://buenosaires.gob.ar/infracciones
Profundidad: 4 niveles
Salida: CSV con URLs inválidas (sin duplicados)
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from datetime import datetime
from collections import deque
import random
import re
import csv
from fake_useragent import UserAgent
import os
import sys

class VerificadorInfracciones:
    def __init__(self):
        self.url_base = "https://buenosaires.gob.ar/infracciones"
        self.dominio_base = "buenosaires.gob.ar"
        self.profundidad_maxima = 3
        self.urls_visitadas = set()
        self.urls_pendientes = deque()
        self.enlaces_invalidos = []  # Solo guardamos los inválidos
        self.urls_invalidas_verificadas = set()  # NUEVO: Para evitar duplicados
        self.tiempo_inicio = datetime.now()
        self.ua = UserAgent()
        
        # Headers para las peticiones
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Sesión de requests para reutilizar conexiones
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def es_dominio_permitido(self, url):
        """Verifica si el dominio de la URL pertenece a buenosaires.gob.ar"""
        try:
            parsed_url = urlparse(url)
            dominio_url = parsed_url.netloc.lower()
            return dominio_url.endswith('buenosaires.gob.ar')
        except:
            return False

    def verificar_estado_http(self, url):
        """Verifica el estado HTTP de una URL y retorna si es válida"""
        try:
            # Headers más completos para parecer un navegador real
            headers_verificacion = {
                'User-Agent': self.ua.random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Charset': 'UTF-8',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Referer': self.url_base
            }
            
            # Primero intentamos con HEAD
            respuesta = self.session.head(
                url, 
                headers=headers_verificacion,
                timeout=15,
                allow_redirects=True
            )
            
            # Considerar códigos de éxito más amplios
            if respuesta.status_code in [200, 301, 302, 303, 307, 308]:
                return True, respuesta.status_code
            
            # Si HEAD no funciona, intentamos con GET
            if respuesta.status_code in [405, 501]:  # Method Not Allowed o Not Implemented
                respuesta = self.session.get(
                    url, 
                    headers=headers_verificacion,
                    timeout=15,
                    allow_redirects=True
                )
                
                # Códigos de éxito para GET
                if respuesta.status_code in [200, 301, 302, 303, 307, 308]:
                    return True, respuesta.status_code
            
            return False, respuesta.status_code
            
        except requests.exceptions.Timeout:
            return False, "TIMEOUT"
        except requests.exceptions.ConnectionError:
            return False, "CONNECTION_ERROR"
        except requests.exceptions.TooManyRedirects:
            return False, "TOO_MANY_REDIRECTS"
        except requests.exceptions.RequestException as e:
            return False, f"REQUEST_ERROR: {str(e)}"
        except Exception as e:
            return False, f"ERROR: {str(e)}"

    def es_enlace_restringido(self, href):
        """Verifica si el enlace está en la lista de restricciones"""
        restricciones = [
            "login.buenosaires.gob.ar",
            ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar",
            "javascript:", "mailto:", "tel:",
            "whatsapp:", "facebook.com", "twitter.com",
            "instagram.com", "linkedin.com", "youtube.com",
            "#", "void(0)"
        ]
        
        href_lower = href.lower()
        for restriccion in restricciones:
            if restriccion in href_lower:
                return True
        return False
    
    def extraer_urls_de_html(self, html, base_url):
        """Extrae todas las URLs del contenido HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        urls = []
        
        # Buscar en diferentes tipos de elementos
        for elemento in soup.find_all(['a', 'button']):
            href = None
            
            # Obtener href
            if elemento.name == 'a':
                href = elemento.get('href')
            elif elemento.name == 'button':
                href = elemento.get('data-href') or elemento.get('onclick')
            
            # Extraer URL de onclick si existe
            if 'onclick' in str(elemento):
                patrones = [
                    r"window.location\s*=\s*['\"]([^'\"]*)['\"]",
                    r"window.open\s*\(\s*['\"]([^'\"]*)['\"]",
                    r"location.href\s*=\s*['\"]([^'\"]*)['\"]",
                    r"location\s*=\s*['\"]([^'\"]*)['\"]"
                ]
                for patron in patrones:
                    match = re.search(patron, str(elemento))
                    if match:
                        href = match.group(1)
                        break
            
            if href and href.strip():
                # Normalizar URL
                if not href.startswith(('http://', 'https://')):
                    href = urljoin(base_url, href)
                
                # Forzar HTTPS para dominios buenosaires.gob.ar
                if href.startswith('http://') and 'buenosaires.gob.ar' in href:
                    href = href.replace('http://', 'https://', 1)
                    print(f"  → Convertido HTTP a HTTPS: {href}")
                
                # Limpiar fragmentos y parámetros innecesarios
                if '#' in href:
                    href = href.split('#')[0]
                
                if href and href != base_url:  # Evitar self-references
                    urls.append(href)
        
        return list(set(urls))  # Eliminar duplicados
    
    def procesar_pagina(self, url, profundidad=0):
        """Procesa una página web y sus enlaces"""
        if profundidad > self.profundidad_maxima:
            return
            
        if url in self.urls_visitadas:
            return
            
        # Verificar que la URL actual pertenece al dominio permitido antes de procesarla
        if not self.es_dominio_permitido(url):
            print(f"⚠ ADVERTENCIA: Intentando procesar URL externa: {url}")
            return
            
        print(f"\n{'='*60}")
        print(f"Procesando: {url}")
        print(f"Profundidad: {profundidad}/{self.profundidad_maxima}")
        print(f"Visitadas: {len(self.urls_visitadas)} | Pendientes: {len(self.urls_pendientes)}")
        print(f"Enlaces inválidos únicos: {len(self.urls_invalidas_verificadas)}")
        print(f"{'='*60}")
        
        self.urls_visitadas.add(url)
        
        try:
            # Obtener contenido de la página
            self.headers['User-Agent'] = self.ua.random
            response = self.session.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            # Extraer URLs
            urls_encontradas = self.extraer_urls_de_html(response.text, url)
            print(f"URLs encontradas en la página: {len(urls_encontradas)}")
            
            for href in urls_encontradas:
                if self.es_enlace_restringido(href):
                    continue
                
                # MODIFICACIÓN: Solo verificar URL si no la hemos verificado antes
                if href not in self.urls_invalidas_verificadas:
                    es_valida, codigo_error = self.verificar_estado_http(href)
                    
                    if not es_valida:
                        print(f"✗ URL INVÁLIDA: {href} (Error: {codigo_error})")
                        # Agregar a la lista de inválidos Y al set de verificadas
                        self.enlaces_invalidos.append({
                            'url_pagina_origen': url,
                            'url_destino_invalida': href,
                            'error': str(codigo_error),
                            'profundidad': profundidad
                        })
                        self.urls_invalidas_verificadas.add(href)
                    else:
                        print(f"✓ URL válida: {href}")
                        
                        # NAVEGACIÓN: SOLO si es del dominio buenosaires.gob.ar
                        if (self.es_dominio_permitido(href) and 
                            href not in self.urls_visitadas and 
                            href not in [u[0] for u in self.urls_pendientes] and 
                            profundidad < self.profundidad_maxima):
                            self.urls_pendientes.append((href, profundidad + 1))
                            print(f"  → Agregada a pendientes para navegación (nivel {profundidad + 1})")
                        elif not self.es_dominio_permitido(href):
                            print(f"  → Enlace externo verificado pero NO NAVEGADO: {href}")
                else:
                    print(f"⚠ URL ya verificada anteriormente: {href}")
                    
        except Exception as e:
            print(f"ERROR procesando página {url}: {str(e)}")

    def verificar_sitio(self):
        """Inicia la verificación recursiva del sitio"""
        print("="*80)
        print("VERIFICADOR DE ENLACES - INFRACCIONES BUENOS AIRES")
        print(f"URL Base: {self.url_base}")
        print(f"Profundidad máxima: {self.profundidad_maxima}")
        print(f"NAVEGACIÓN: Solo dentro de buenosaires.gob.ar")
        print(f"VERIFICACIÓN: Todos los enlaces encontrados (internos y externos)")
        print(f"DUPLICADOS: Eliminados automáticamente del reporte")
        print(f"Fecha/Hora inicio: {self.tiempo_inicio.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        self.urls_pendientes.append((self.url_base, 0))
        
        while self.urls_pendientes:
            url_actual, profundidad = self.urls_pendientes.popleft()
            if profundidad <= self.profundidad_maxima:
                self.procesar_pagina(url_actual, profundidad)
            
            # Cada 50 páginas procesadas, mostramos progreso
            if len(self.urls_visitadas) % 50 == 0 and len(self.urls_visitadas) > 0:
                print(f"\n*** PROGRESO: {len(self.urls_visitadas)} páginas procesadas ***")
                print(f"*** Enlaces inválidos únicos: {len(self.urls_invalidas_verificadas)} ***\n")
        
        self.generar_informe()

    def generar_informe(self):
        """Genera el archivo CSV con los enlaces inválidos (sin duplicados)"""
        print("\n" + "="*80)
        print("GENERANDO INFORME FINAL...")
        
        tiempo_fin = datetime.now()
        duracion = tiempo_fin - self.tiempo_inicio
        
        # MODIFICACIÓN: Crear diccionario para eliminar duplicados por URL destino
        # Mantenemos solo la primera ocurrencia de cada URL inválida
        enlaces_unicos = {}
        for enlace in self.enlaces_invalidos:
            url_destino = enlace['url_destino_invalida']
            if url_destino not in enlaces_unicos:
                enlaces_unicos[url_destino] = enlace
        
        # Convertir de vuelta a lista
        enlaces_sin_duplicados = list(enlaces_unicos.values())
        
        # Crear archivo CSV con enlaces inválidos únicos
        nombre_archivo = f'enlaces_invalidos_infracciones_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        
        with open(nombre_archivo, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            
            # Encabezado
            writer.writerow(['URL_PAGINA_ORIGEN', 'URL_DESTINO_INVALIDA'])
            
            # Datos sin duplicados
            for enlace in enlaces_sin_duplicados:
                writer.writerow([enlace['url_pagina_origen'], enlace['url_destino_invalida']])
        
        # Crear archivo de resumen
        nombre_resumen = f'resumen_verificacion_infracciones_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        
        with open(nombre_resumen, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(['Concepto', 'Valor'])
            writer.writerow(['URL Base', self.url_base])
            writer.writerow(['Fecha inicio', self.tiempo_inicio.strftime('%Y-%m-%d %H:%M:%S')])
            writer.writerow(['Fecha fin', tiempo_fin.strftime('%Y-%m-%d %H:%M:%S')])
            writer.writerow(['Duración total', str(duracion)])
            writer.writerow(['Profundidad máxima', self.profundidad_maxima])
            writer.writerow(['Páginas visitadas', len(self.urls_visitadas)])
            writer.writerow(['Enlaces inválidos totales encontrados', len(self.enlaces_invalidos)])
            writer.writerow(['Enlaces inválidos únicos en CSV', len(enlaces_sin_duplicados)])
            writer.writerow(['Duplicados eliminados', len(self.enlaces_invalidos) - len(enlaces_sin_duplicados)])
            
            # Resumen por profundidad
            writer.writerow([])
            writer.writerow(['Profundidad', 'Enlaces Inválidos Únicos'])
            for prof in range(self.profundidad_maxima + 1):
                invalidos = sum(1 for e in enlaces_sin_duplicados if e['profundidad'] == prof)
                writer.writerow([f'Nivel {prof}', invalidos])
        
        # Mostrar resumen final
        print("PROCESO COMPLETADO!")
        print(f"Archivo generado: {nombre_archivo}")
        print(f"Resumen generado: {nombre_resumen}")
        print(f"Páginas visitadas: {len(self.urls_visitadas)}")
        print(f"Enlaces inválidos totales: {len(self.enlaces_invalidos)}")
        print(f"Enlaces inválidos únicos en CSV: {len(enlaces_sin_duplicados)}")
        print(f"Duplicados eliminados: {len(self.enlaces_invalidos) - len(enlaces_sin_duplicados)}")
        print(f"Duración total: {duracion}")
        print("="*80)
        
        if enlaces_sin_duplicados:
            print("\nPREVIEW - Primeros 10 enlaces inválidos únicos:")
            for i, enlace in enumerate(enlaces_sin_duplicados[:10], 1):
                print(f"{i:2d}. {enlace['url_pagina_origen']} → {enlace['url_destino_invalida']}")
            if len(enlaces_sin_duplicados) > 10:
                print(f"... y {len(enlaces_sin_duplicados) - 10} más en el archivo CSV")

    def cerrar(self):
        """Cierra la sesión de requests"""
        self.session.close()


if __name__ == "__main__":
    print("Iniciando verificador de enlaces para infracciones...")
    
    verificador = VerificadorInfracciones()
    
    try:
        verificador.verificar_sitio()
    except KeyboardInterrupt:
        print("\n\nVerificación interrumpida por el usuario.")
        print("Guardando resultados parciales...")
        verificador.generar_informe()
    except Exception as e:
        print(f"\n\nError durante la verificación: {str(e)}")
        print("Guardando resultados parciales...")
        verificador.generar_informe()
    finally:
        verificador.cerrar()
    
    print("\nVerificación completada.")
    input("Presiona Enter para salir...")