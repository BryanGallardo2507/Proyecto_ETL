import pyodbc
import logging

# Configurar logs
logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Conexión a SQL Server
conn = pyodbc.connect("DRIVER={SQL Server};SERVER=tu_servidor;DATABASE=tu_base;UID=tu_usuario;PWD=tu_password")
cursor = conn.cursor()

def ejecutar_etl(query_obj):
    """ Ejecuta extracción y carga de datos en SQL Server """
    try:
        cursor.execute(query_obj.extract_query)
        data = cursor.fetchall()

        if not data:
            logging.warning(f"No hay datos para cargar en {query_obj.load_query}")
            return

        cursor.executemany(query_obj.load_query, data)
        conn.commit()
        logging.info(f"{len(data)} registros cargados en {query_obj.load_query}")

    except Exception as e:
        logging.error(f"Error en {query_obj.load_query}: {str(e)}")
        conn.rollback()

# Ejecutar ETL automáticamente para todas las tablas
for query in sqlserver_queries:
    ejecutar_etl(query)

cursor.close()
conn.close()
logging.info("Proceso ETL completado")

