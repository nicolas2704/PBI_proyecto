import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
from pathlib import Path
import os
import openpyxl
from dotenv import load_dotenv
load_dotenv()


# librerias airflow
# definen la programacion del DAG cada cuanto se repetira,
# Es un objeto que representa un intervalo de tiempo, es decir, una cantidad de días, horas, minutos, segundos
from datetime import timedelta
# el objeto del DAG que necesitara para instanciar un DAG
# importa la clase DAG
from airflow.models import DAG
# Operadores, se necesita para escribir tareas
# estos definen que hace cada tarea en su DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
POSTGRES_CONN_ID = os.getenv("conex_db") # para la conexion a la base de datos

# funciones
def extraer():
    url=os.getenv("url_web") # para la conexion a la base de datos
    dataframe=pd.DataFrame(columns=["Paises", "PBI"]) #dataframe vacio
    excel_ruta1=Path("/home/nicolas27/data/ETL/airflow/proyectos_airflow/proyectos_dags/proyectos_completos/web_scraping_proj/Paises_PBI_extraido.xlsx") #ruta donde se guardara el archivo csv
    proceso_log("Proceso ETL inicializando:")
    proceso_log("Proceso Extraccion Inicializando:")

    pagina_html=requests.get(url).text #hace una solicitud get a la url y dejando el contenido en texto
    datos=BeautifulSoup(pagina_html,"html.parser") # crea un objeto soup para acceder al html
    tabla=datos.find_all("tbody") #encuentra todos los cuerpos de la tabla de las paginas
    filas=tabla[2].find_all("tr") # busca las filas de la tabla 3 y las guarda

    # iteraciones
    for fila in filas:
        celda=fila.find_all("td") #busca las celdas de cada fila y las guarda
        if len(celda)!=0: # si la celda no esta vacia
            paises=str(celda[0].text.strip()) # agarra el contenido de tipo texto la columna 1 la transforma en string y el strip elimina cualquier espacio en blanco o si hay 2 elementos en 1 celda 
            pbi=celda[2].contents[0].text.replace(",","").replace("—","") # agarra la columna 3 hace lo mismo que lo anterior pero reemplaza caracteres en la celda
            data_dict={"Paises":paises,
                       "PBI":pbi}
            dataframe1=pd.DataFrame(data_dict, index=[0]) # convierte el dic en un dataframe
            dataframe=pd.concat([dataframe, dataframe1], ignore_index=True) # une el dataframe vacio con el anterior no superponiendo sus indices
        else:
            continue
    dataframe.to_excel(excel_ruta1) # convierte el dataframe en un archivo excel
    proceso_log("Proceso Extraccion Finalizado\n")
    

# transformar
def transformar():
    proceso_log("Proceso de Transfromacion Inicializado:")
    excel_ruta1=Path("/home/nicolas27/data/ETL/airflow/proyectos_airflow/proyectos_dags/proyectos_completos/web_scraping_proj/Paises_PBI_extraido.xlsx") #ruta donde se guardara el archivo csv
    excel_ruta2=Path("/home/nicolas27/data/ETL/airflow/proyectos_airflow/proyectos_dags/proyectos_completos/web_scraping_proj/Paises_PBI_transformado.xlsx") #ruta donde se guardara el archivo csv
    dataframe=pd.read_excel(excel_ruta1)
    dataframe=dataframe.iloc[1:,0:] # elimina la fila 1 del dataframe
    dataframe=dataframe.iloc[:,1:] # elimina la primera columna
    dataframe = dataframe.rename(columns={'Unnamed: 0': 'Posicion'})
    dataframe["PBI"]=pd.to_numeric(dataframe["PBI"],errors="coerce") #convierte los datos de las celdas a numerico y en el caso de valores nulos o vacios los omite poniendoles NaN
    dataframe["PBI"]=dataframe["PBI"]/1000 # el valor anterior numerico entero lo transforma a decimal
    dataframe["PBI"]=np.round(dataframe["PBI"],2) # le saca valores decimales dejandolo solo en 2
    dataframe.to_excel(excel_ruta2) # convierte el dataframe en un archivo excel
    proceso_log("Proceso de Transformacion Finalizado\n")

# cargar
def cargar():
    proceso_log("Proceso de Carga Inicializado:")
    excel_ruta2=Path("/home/nicolas27/data/ETL/airflow/proyectos_airflow/proyectos_dags/proyectos_completos/web_scraping_proj/Paises_PBI_transformado.xlsx") #ruta donde se guardara el archivo csv
    dataframe=pd.read_excel(excel_ruta2)
    nombre_tabla="paises_pbi" #nombre de la tabla en la base de datos
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn_uri = pg_hook.get_uri()
        # Dentro de la función 'cargar' antes de crear el engine:
        engine = create_engine(conn_uri)
        print("conexion exitosa a la BD")
        dataframe.to_sql(nombre_tabla, engine, if_exists='replace', index=False) # crea la tabla, si existe la reemplaza y carga los datos
        print("Datos cargados correctamente en la Base de Datos")
        proceso_log("Carga en Base de datos PostgreSQL")

    except Exception as ex:
        print(ex)
        print("No se pudieron cargar los datos")
        proceso_log(f"Error durante la carga: {ex}")
        raise # fuerza el fallo en caso de error

    proceso_log("Proceso de Carga Finalizado")
    proceso_log("Proceso ETL Finalizado\n")

#registro de procesos
def proceso_log(mensaje):
    archivo_registro="/home/nicolas27/data/ETL/airflow/proyectos_airflow/proyectos_dags/proyectos_completos/web_scraping_proj/etl_proyect_log.txt" #archivo que contendra los registros
    formato_tiempo="%Y-%h-%d %H:%M:%S" #forma de fecha deseado
    fecha_actual=datetime.now() # fecha actual
    marca_de_tiempo=fecha_actual.strftime(formato_tiempo) # transformacion de la fecha
    with open(archivo_registro, "a") as registro: #abre o crea el archivo
        registro.write(marca_de_tiempo+ " , "+ mensaje+"\n") # escribe en el archivo

# especificacion de argumentos del DAG
# se pueden anular por tarea durante la inicializacion del operador
default_args ={
    "owner":"Nicolas",
    "start_date": pendulum.today('UTC').add(days=-1),# decide la fecha de inicio 1 se ejecuta inmediatamente 0 se ejecuta a las 00:00 del otro dia
    #cantidad de veces que debe intentar si falla
    "retries":0,
    # tiempo de espera entre intentos
    "retry_delay":timedelta(minutes=5),
}

# definir el DAG
dag = DAG(
    # nombre del DAG
    "PBI-DAG-proyecto",
    # argumentos del diccionario anterior
    default_args=default_args,
    # descripcion del flujo de trabajo
    description="Datos del PBI",
    # instrucciones de programacion
    # el DAG se ejecutara cada 1 dia una vez implementado
    schedule="0 0 * * *",  # Ejecutar diariamente a la medianoche
)

#tarea 1 
# define la tarea llamada ejecutar_extraccion y llama a la funcion extraer
ejecutar_extraccion = PythonOperator(
    # id de la tarea
    task_id="extraer",
    # lo que realizara la tarea en este caso ejecutar la funcion extraer
    python_callable=extraer,
    # la tarea se asigna al DAG definido anteriormente 
    dag=dag,
)

#tarea 2
# define la tarea llamada ejecutar_transformacion y llama a la funcion transformar
ejecutar_transformacion = PythonOperator(
    # id de la tarea
    task_id="transformar",
    # lo que realizara la tarea en este caso ejecutar la funcion transformar
    python_callable=transformar,
    # la tarea se asigna al DAG definido anteriormente 
    dag=dag,
)

#tarea 3
# define la tarea llamada ejecutar_carga y llama a la funcion cargar
ejecutar_carga = PythonOperator(
    # id de la tarea
    task_id="cargar",
    # lo que realizara la tarea en este caso ejecutar la funcion transformar
    python_callable=cargar,
    # la tarea se asigna al DAG definido anteriormente 
    dag=dag,
)
# flujo de trabajo (dependencias)
ejecutar_extraccion >> ejecutar_transformacion >> ejecutar_carga

# cosas a agregar
# realizar consulta a la base de datos y luego notificar por mail