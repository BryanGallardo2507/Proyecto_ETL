import pandas as pd
import logging
from sqlalchemy import create_engine
from db_credentials import datawarehouse_name

# Configurar logs
logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_sqlalchemy_engine(connection_config):
    """ Crear una conexión SQLAlchemy a la base de datos """
    connection_string = f"mssql+pyodbc://{connection_config['user']}:{connection_config['password']}@{connection_config['server']}:{connection_config['port']}/{connection_config['database']}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(connection_string)
    return engine

def etl(query, source_engine, target_engine):
    """ Ejecuta extracción, transformación y carga de datos usando pandas y SQLAlchemy """
    try:
        data = pd.read_sql(query.extract_query, source_engine)
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
            with target_engine.connect() as conn:
                conn.execute(f"USE {datawarehouse_name}")
                conn.execute(query.load_query, data_tuples)
            logging.info(f"Cargados {len(data_tuples)} registros en destino")
        except Exception as e:
            logging.error(f"Error en la carga de datos: {e}")
            logging.error(f"Consulta: {query.load_query}")
            if data_tuples:
                logging.error(f"Primer registro: {data_tuples[0]}")
    else:
        logging.warning("No hay datos para cargar")

def etl_process(queries, target_db_config, source_db_config):
    """ Proceso ETL para todas las consultas utilizando SQLAlchemy """
    try:
        # Crear motores SQLAlchemy para la base de datos fuente y destino
        source_engine = create_sqlalchemy_engine(source_db_config)
        target_engine = create_sqlalchemy_engine(target_db_config)
        logging.info(f"Conectado a la base de datos fuente: {source_db_config['database']}")
    except Exception as e:
        logging.error(f"Error de conexión a la base fuente: {e}")
        return
    
    for query in queries:
        logging.info(f"Procesando consulta: {query.extract_query[:100]}...")
        etl(query, source_engine, target_engine)
    
    logging.info("Proceso ETL finalizado")
