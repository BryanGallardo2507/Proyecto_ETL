import streamlit as st
from sqlalchemy import create_engine
from etl import etl_process
from db_credentials import datawarehouse_db_config, sqlserver_db_config
from sql_queries import sqlserver_queries

# Título de la aplicación
st.title("ETL Data Warehouse - Proceso de Carga")

# Función para crear la conexión a la base de datos usando SQLAlchemy
def create_sqlalchemy_engine(connection_config):
    """ Crear una conexión SQLAlchemy a la base de datos """
    # Si no existe 'port' en la configuración, eliminarla
    port = connection_config.get('port', 1433)  # Asignar el puerto 1433 por defecto si no se encuentra
    connection_string = f"mssql+pyodbc://{connection_config['user']}:{connection_config['password']}@{connection_config['server']}:{port}/{connection_config['database']}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(connection_string)
    return engine

# Función para ejecutar el proceso ETL
def run_etl_process():
    """ Ejecuta el proceso ETL cuando el usuario lo solicita """
    try:
        st.write("Conectando a la base de datos de destino...")

        # Crear la conexión usando SQLAlchemy
        target_engine = create_sqlalchemy_engine(datawarehouse_db_config)
        st.success("Conectado a la base de datos de destino (DW_CHINOOK)")

        # Crear conexión para la base de datos de origen (SQL Server)
        source_engine = create_sqlalchemy_engine(sqlserver_db_config)

        # Ejecutar el proceso ETL con las consultas definidas
        etl_process(sqlserver_queries, datawarehouse_db_config, sqlserver_db_config)
        
        # Cerrar las conexiones
        target_engine.dispose()
        source_engine.dispose()
        
        st.success("Proceso ETL completado exitosamente.")
    except Exception as e:
        st.error(f"Error en el proceso ETL: {e}")

# Crear una interfaz de usuario para ejecutar el proceso ETL
if st.button("Ejecutar Proceso ETL"):
    run_etl_process()