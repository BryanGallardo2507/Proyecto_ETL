from etl import etl_process
from db_credentials import datawarehouse_db_config, sqlserver_db_config
from sql_queries import sqlserver_queries

def main():
    print("""
    ****************************************
    * ETL CON PYODBC - CARGA DW_CHINOOK    *
    ****************************************
    """)
    
    try:
        print("[1/3] Configurando entorno ETL...")
        print("[2/3] Ejecutando proceso... (Ver etl_pyodbc.log para detalles)")
        
        etl_process(
            queries=sqlserver_queries,
            target_config=datawarehouse_db_config,
            source_config=sqlserver_db_config
        )
        
        print("[3/3] ¡Proceso completado con éxito!")
    
    except Exception as e:
        print(f"\n[ERROR] {str(e)}\n")
        print("Revise el archivo etl_pyodbc.log para detalles técnicos")
        exit(1)

if __name__ == "_main_":
    main()