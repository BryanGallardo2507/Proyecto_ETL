import pyodbc
import pandas as pd
import logging
from db_credentials import datawarehouse_name

# Configurar logs
logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def etl(query, source_cnx, target_cnx):
    """ Ejecuta extracción, transformación y carga de datos usando pandas """
    try:
        data = pd.read_sql(query.extract_query, source_cnx)
        logging.info(f"Extraídos {len(data)} registros de: {query.extract_query[:50]}...")
    except Exception as e:
        logging.error(f"Error en la extracción de datos: {e}")
        return
    
    if not data.empty:
        try:
            # Convertir columnas datetime
            datetime_cols = data.select_dtypes(include=['datetime64']).columns
            for col in datetime_cols:
                data[col] = pd.to_datetime(data[col])
            
            # Manejo de valores NULL
            data = data.where(pd.notnull(data), None)
            
            # Convertir a lista de tuplas para carga masiva
            data_tuples = [tuple(x) for x in data.to_numpy()]
            
            # Carga de datos
            target_cursor = target_cnx.cursor()
            target_cursor.execute(f"USE {datawarehouse_name}")
            target_cursor.executemany(query.load_query, data_tuples)
            target_cnx.commit()
            target_cursor.close()
            logging.info(f"Cargados {len(data_tuples)} registros en destino")
        except Exception as e:
            logging.error(f"Error en la carga de datos: {e}")
            logging.error(f"Consulta: {query.load_query}")
            if data_tuples:
                logging.error(f"Primer registro: {data_tuples[0]}")
    else:
        logging.warning("No hay datos para cargar")

def etl_process(queries, target_cnx, source_db_config):
    """ Proceso ETL para todas las consultas """
    try:
        source_cnx = pyodbc.connect(**source_db_config)
        logging.info(f"Conectado a la base de datos fuente: {source_db_config['database']}")
    except Exception as e:
        logging.error(f"Error de conexión a la base fuente: {e}")
        return
    
    for query in queries:
        logging.info(f"Procesando consulta: {query.extract_query[:100]}...")
        etl(query, source_cnx, target_cnx)
    
    source_cnx.close()
    logging.info("Proceso ETL finalizado")
