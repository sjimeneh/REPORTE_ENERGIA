import asyncio
from pyppeteer import launch
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from dotenv import dotenv_values
import pyfiglet
import warnings
import mysql.connector
import numpy as np
#Librerias para Caribemar
import requests
import zipfile
import os
import shutil
import urllib3
import math

# Deshabilitar advertencias de verificación de SSL
urllib3.disable_warnings() 

#Ignorar Errores
warnings.filterwarnings("ignore")
#Cargar Variables de Entorno
env_vars = dotenv_values('.env')

async def insertar_datos_bd(consulta_insertar,datos,consulta_truncar):
    #LINUX
    mydb = mysql.connector.connect(
        host=env_vars['DB_LINUX_HOST_ENERGIA'],
        user=env_vars['DB_LINUX_USER_ENERGIA'],
        password=env_vars['DB_LINUX_PASS_ENERGIA'],
        database=env_vars['DB_LINUX_DATABASE_ENERGIA']
        )
    mycursor = mydb.cursor()

    try:
        #Trucar la tabla
        mycursor.execute(consulta_truncar)
        mydb.commit()

        # Ejecución de la inserción masiva
        mycursor.executemany(consulta_insertar, datos)
        #Confirmo el commit en la base de datos
        mydb.commit()
        # Obtener el número de filas afectadas (datos insertados)
        num_filas_insertadas = mycursor.rowcount
        await Mensaje(f"Se insertaron {num_filas_insertadas} filas de {len(datos)}")
    except Exception as e:
        await Mensaje("BD-Error al Insertar los datos"+str(e))
    finally:
        mycursor.close()
        mydb.close()

async def Mensaje(mensaje=""):
    print("\n%s\n##########################################################################################################"
     %mensaje)

async def titulo(texto , formato="starwars"):
    figlet_font = pyfiglet.Figlet(font=formato)
    art_text = figlet_font.renderText(texto)
    print(art_text)
    return art_text

async def generar_fecha():
    # Obtener la fecha actual
    fecha_actual = datetime.now()

    # Extraer día, mes y año de la fecha actual
    dia = fecha_actual.day
    mes = fecha_actual.month
    año = fecha_actual.year

    # Formatear la fecha en el formato requerido (dd-mm-yyyy)
    fecha_formateada = f"{dia}-{mes}-{año}"

    return fecha_formateada

async def consultar_informacion_aire():

    await Mensaje("Extrayendo Datos de AIR-E")

    # Lanzar el navegador
    browser = await launch(
        ignoreHTTPSErrors=True,
       # executablePath='/root/.cache/puppeteer/chrome/linux-114.0.5735.90/chrome-linux64/chrome',
        args=['--no-sandbox',
        '--proxy-server=http://10.158.122.48:8080',
        '--disable-infobars',  # Deshabilitar la barra de información (como las cookies)
        '--disable-notifications',  # Deshabilitar las notificaciones del navegador
        '--disable-extensions',
        '--disable-gpu',
        '--no-sandbox',
        '--disable-dev-shm-usage',
        ], 
            options={
                # 'headless': False,  # Habilita la visualización del navegador
                'userDataDir': "temporal_data" #Cambiar la ruta de los datos temporales
            }
            )

    page = await browser.newPage()
    # Establecer las dimensiones de la ventana igual a las dimensiones de la pantalla
    await page.setViewport({'width': 1080, 'height': 1080})
    
    try:
        formato_url = env_vars["URL_BASE_AIRE"]
        #Anexo la fecha de consulta a la URL
        formato_url = formato_url.replace("oFecha",await generar_fecha())
        
        await page.goto(formato_url)

        #Si existe el boton cookie, aceptelo
        boton_coockie = await page.querySelector(env_vars["BOTON_COOKIE"])
        if(boton_coockie):
            await Mensaje("Aceptando Las Cookies de AIR-E")
            await boton_coockie.click()

        await Mensaje("Extrayendo los datos de la tabla AIR-E")
        # Obtener todos los elementos con la clase "ax-tabs-acordeon__item"
        elements = await page.querySelectorAll(env_vars["DIV_ELEMENTO_CIUDADES"])

        datos_ingresar = []

        # Hacer clic en cada elemento
        for element in elements:
            
            #Obtengo el nombre del Municipio
            nombre_municipio = await page.evaluate('(element) => element.innerText', element)

            #Hago click en cada uno de ellos
            await element.click()

            #Espero 2 segundos mientras se cargan los datos de cada tabla
            await asyncio.sleep(1)
            

            # Obtener el contenido HTML del elemento
            html_content = await page.evaluate('(element) => element.innerHTML', element)
            tabla_df =  pd.read_html(html_content)

            for tabla in tabla_df:
                tabla_matriz = tabla.values.flatten()
                #Como tabla.values me devuelve todos los datos de la tabla en un solo array
                #Divido en array en secciones de 8 para generar un subArray y así obtener los datos de cada fila
                subarrays = [tabla_matriz[i:i + 8] for i in range(0, len(tabla_matriz), 8)]

                await Mensaje(f"Filtrando los datos de {nombre_municipio} = {str(len(subarrays))} Filas")

                #Ahorra recorre cada array para poder obtener los elementos y añadirles el municipio
                for array in subarrays:
                    array = await limpiar_datos(array)

                    #Limpiar datos
                    for i in range(len(array)):
                        if str(array[i]) == "nan":
                            array[i] = 0

                    # Sintaxis: numpy.insert(array, indice, valor)
                    array = np.insert(array, 0, nombre_municipio)
                    # print(array)
                    datos_ingresar.append(array.tolist())


        if(len(datos_ingresar)>0):
            await Mensaje(f"Enviando datos a la tabla AIR_E")
            await insertar_datos_bd(env_vars["BD_CONSULTA_INSERTAR_DATOS_AIRE"],datos_ingresar,env_vars['BD_CONSULTA_TRUNCAR_TABLA_AIRE'])
            

    except Exception as e:
        await Mensaje("Ha surgido un error"+str(e))
    finally:
        await page.close()
        await browser.close()

async def numero_semana_del_anio():
    fecha_actual = datetime.now()

    # Obtenemos el número de la semana del año utilizando el atributo isocalendar de la fecha
    numero_semana = fecha_actual.isocalendar()[1]
    return numero_semana

#Descargar el archivo .ZIP
async def descargar_archivo_zip(archivo_zip):
    try:

        proxies = {
            'http': env_vars['HTTP_PROXYUNE'],
            'https': env_vars['HTTP_PROXYUNE'],
        }

        url_base_caribemar = env_vars['URL_BASE_CARIBEMAR']
        url_base_caribemar = url_base_caribemar.replace("oNumeroSemena",str(await numero_semana_del_anio()))
        # url_base_caribemar = url_base_caribemar.replace("oNumeroSemena",str(29))
        await Mensaje(f"Descargando archivo ZIP de {url_base_caribemar}")
        #Hago la peticion get pasandole el proxy
        #Por temas de proxy si o si debo deshabilitar el varify
        response = requests.get(url_base_caribemar,proxies=proxies,verify=False)

        if response.status_code == 200:
            with open(archivo_zip, "wb") as archivo_local:
                archivo_local.write(response.content)
            await Mensaje(f"Archivo descargado con éxito: {archivo_zip}")
            return True
        else:
            await Mensaje(f"Error al descargar el archivo. Código de estado: {response.status_code}")
            return False
    except Exception as e:
        await Mensaje(f"Ocurrió un error al Descargar el archivo .ZIP : {str(e)}")
        return False 

# Extraer los datos en la carpeta de destino
#Este metodo no se usa
async def extraer_informacion_zip():
    try:
        archivo_zip = env_vars['NOMBRE_ARCHIVO_ZIP']
        carpeta_destino = env_vars['CARPETA_DATOS_EXTRAIDOS']
        
        # Verificar si el archivo ZIP existe antes de intentar extraerlo
        if not os.path.exists(archivo_zip):
            await Mensaje(f"El archivo ZIP '{archivo_zip}' no existe.")
            return False

        # Crear la carpeta de destino si no existe
        if not os.path.exists(carpeta_destino):
            os.makedirs(carpeta_destino)

        # Extraer los archivos del ZIP en la carpeta de destino
        with zipfile.ZipFile(archivo_zip, "r") as zip_ref:
            await Mensaje(f"Extrayendo todo los archivos")
            zip_ref.extractall(carpeta_destino)

        await Mensaje(f"Datos extraídos y ubicados en la carpeta de destino '{carpeta_destino}'")
        return True

    except FileNotFoundError as e:
        await Mensaje(f"Error: El archivo ZIP no se encontró: {str(e)}")
        return False

    except Exception as e:
        await Mensaje(f"Ocurrió un error al Extraer los Archivos: {str(e)}")
        return False

#Estraer solo los archivos de excel cuyo nombre contengan la palabra interrupcion del archivo .zip
async def extraer_xlsx_desde_zip(archivo_zip, carpeta_destino):
    # Crea la carpeta de destino si no existe
    os.makedirs(carpeta_destino, exist_ok=True)

    try:
        # Abre el archivo ZIP en modo lectura
        with zipfile.ZipFile(archivo_zip, 'r') as zip_file:
            # Obtén una lista de todos los archivos en el ZIP
            lista_archivos = zip_file.namelist()

            # Filtra los archivos .xlsx y extrae cada uno a la carpeta de destino
            for archivo in lista_archivos:
                # Extrae el archivo .xlsx en la carpeta de destino
                nombre_archivo = os.path.basename(archivo)
                if archivo.lower().endswith('.xlsx') and "interrupcion" in nombre_archivo.lower():
                    
                    # Obtiene solo el nombre del archivo sin la ruta interna del ZIP
                    ruta_destino = os.path.join(carpeta_destino, nombre_archivo)
                    with open(ruta_destino, 'wb') as archivo_destino:
                        archivo_destino.write(zip_file.read(archivo))
            await Mensaje(f"Archivos XLSX extraidos en la carpeta : {carpeta_destino}")
            return True
    except Exception as e:
        await Mensaje(f"Error al extraer archivos desde el ZIP: {str(e)}")
        return False

#Eliminar el archivo .zip
async def eliminar_archivo_zip(archivo_zip):
    # Eliminar el archivo .zip después de la extracción
    os.remove(archivo_zip)
    await Mensaje(f"Archivo {archivo_zip} Eliminado.")

#Eliminar Archiovos Sobrantes
async def eliminar_carpeta_con_contenido(carpeta_path):
    try:
        shutil.rmtree(carpeta_path)
        await Mensaje(f"La carpeta {carpeta_path} y su contenido han sido eliminados exitosamente.")
    except OSError as e:
        await Mensaje(f"Error al eliminar la carpeta {carpeta_path}: {e}")

#Organizar los archivos .xlsx y solo dejar aquellos cuyo nombre contenga la palabra Interrupcion
async def archivos_finales():
    auxiliar = False
    try:
        # Crear la carpeta "archivos_finales" si no existe
        if not os.path.exists(env_vars['CARPETA_ARCHIVOS_FINALES']): 
            os.makedirs(env_vars['CARPETA_ARCHIVOS_FINALES'])

        # Recorremos todos los archivos y carpetas dentro de "datosEnergia"
        for root, _, files in os.walk(env_vars['CARPETA_DATOS_EXTRAIDOS']):
            for file in files:
                # Verificamos si el archivo es un archivo .xlsx y contiene la palabra "interrupcion" en su nombre
                if file.lower().endswith('.xlsx') and 'interrupcion' in file.lower():
                    auxiliar = True
                    origen = os.path.join(root, file)  # Ruta completa del archivo actual
                    destino = os.path.join(env_vars['CARPETA_ARCHIVOS_FINALES'], file)  # Ruta completa del destino
                    shutil.move(origen, destino)  # Movemos el archivo a la carpeta "archivos_finales"

        await Mensaje(f"Archivos copiados a la carpeta '{env_vars['CARPETA_ARCHIVOS_FINALES']}'.")
        return auxiliar
    
    except Exception as e:
        print(f"Ocurrió un error: {str(e)}")
        return False

async def leer_archivos_xlsx(carpeta):
    try:
        #Declaro el array con el cual insertaré los datos posteriormente
        array_datos_insertar = []

        #Recorro cada uno de los archivo y proceso solo los archivo .xlsx
        for archivo in os.listdir(carpeta):
            if archivo.endswith(".xlsx"): 
                try:
                    ruta_archivo = os.path.join(carpeta, archivo)
                    await procesar_archivo_excel(ruta_archivo,array_datos_insertar)
                except Exception as e:
                    print(f"ERROR - [ERROR LECTURA EXCEL]\nMENSAJE - [{str(e)}]")
        
        #Inserto los datos en la base de datos en caso de que se hayan cargado
        if(len(array_datos_insertar)>0):
            await insertar_datos_bd(env_vars['BD_CONSULTA_INSERTAR_DATOS_CARIBEMAR'],array_datos_insertar,env_vars['BD_CONSULTA_TRUNCAR_TABLA_CARIBEMAR'])
    except Exception as e:
        print(f"ERROR - [LECTURA CARPETA]\nMENSAJE - [{str(e)}]")

#Metodo para recorrer cada una de las hoja, hace llamdado al metodo procesar_hoja
async def procesar_archivo_excel(ruta_archivo,array_datos_insertar):
    await Mensaje("Procesando archivo: "+ ruta_archivo)
    xls = pd.ExcelFile(ruta_archivo)

    #Recorro cada una de las hoja y proceso los datos
    for nombre_hoja in xls.sheet_names:
        #Convierto los datos de la hoja en un dataFrame para poderlo manipular
        df = xls.parse(nombre_hoja)
        await procesar_hoja(df,nombre_hoja,array_datos_insertar)

#Metodo para recorrer cada una de las hojas e insertar los datos
async def procesar_hoja(dataframe,nombre_hoja,array_datos_insertar):
    #Obtengo la hoja y proceso cada una de las filas de hoja
    for indice, fila in dataframe.iterrows():
        if indice == 0:
            continue
        #Ingreso los valores en el array para poderlos insertar previamente
        datos = [str(fila[0]),str(fila[1]),str(fila[2]),str(fila[3]),str(fila[4]),str(fila[5]),str(fila[6]),str(fila[7]),str(fila[8]),str(fila[9]),str(fila[10]),str(fila[11]),str(fila[12]),str(fila[13]),str(fila[14]),str(nombre_hoja)]
        datos = await limpiar_datos(datos)
        array_datos_insertar.append(datos)

    await Mensaje(f"{str(len(dataframe))} Filas Procesadas de la Hoja {str(nombre_hoja)}")

async def limpiar_datos(datos):
    for i in range(len(datos)):
        datos[i] = str(datos[i]).replace("\'","").replace("\"","")
    return datos

#Metodo subMain que descarga el archivo zip, extrae los xlsx, los procesa en inserta los datos en la BD
async def consultar_informacion_cariber():
    archivo_zip = env_vars['NOMBRE_ARCHIVO_ZIP']
    carpeta_destino = env_vars['CARPETA_DATOS_EXTRAIDOS']
    try:
        if(await descargar_archivo_zip(archivo_zip)):
            asyncio.sleep(2)
            if(await extraer_xlsx_desde_zip(archivo_zip,carpeta_destino)):
                await asyncio.sleep(2)
                await eliminar_archivo_zip(archivo_zip)
                await asyncio.sleep(2)
                await leer_archivos_xlsx(carpeta_destino)
                await asyncio.sleep(2)
                await eliminar_carpeta_con_contenido(carpeta_destino)
                await asyncio.sleep(2)
    except Exception as e:
        print(f"Ocurrió un error en el Hilo Principal: {str(e)}")

async def main():
    try:
        await titulo("ENERGIA")
        await consultar_informacion_aire()
        await asyncio.sleep(5)
        await consultar_informacion_cariber()
        await Mensaje("El Programa Ha Finalizado Exitosamente")
    except Exception as e:
        await Mensaje(f"Error en el Hilo Principal {e}")

asyncio.get_event_loop().run_until_complete(main())
        
