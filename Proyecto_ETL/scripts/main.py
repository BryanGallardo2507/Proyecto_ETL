
import pyodbc
from etl import etl_process
from db_credentials import datawarehouse_name, source_db_config, target_db_config
from sql_queries import sqlserver_queries  # Asegúrate de que tienes las consultas definidas

def main():
    """ Ejecuta el proceso ETL para cargar los datos en la base de datos DW_CHINOOK """
    try:
        # Conexión a la base de datos de destino (DW_CHINOOK)
        target_cnx = pyodbc.connect(**target_db_config)
        print("Conectado a la base de datos de destino (DW_CHINOOK)")
    except Exception as e:
        print(f"Error al conectar a la base de datos de destino: {e}")
        return

    # Ejecutar el proceso ETL con las consultas definidas
    etl_process(sqlserver_queries, target_cnx, source_db_config)

    # Cerrar la conexión de destino
    target_cnx.close()
    print("Proceso ETL completado.")

if __name__ == "__main__":
    main()

