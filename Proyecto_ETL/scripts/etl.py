import pyodbc
import logging
from datetime import datetime

# Configura logging
logging.basicConfig(
    filename='etl_pyodbc.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_connection(db_config):
    """Crea conexión pyodbc"""
    conn_str = (
        f"DRIVER={{{db_config['driver']}}};"
        f"SERVER={db_config['server']};"
        f"DATABASE={db_config['database']};"
        f"UID={db_config['user']};"
        f"PWD={db_config['password']};"
        f"PORT={db_config['port']};"
    )
    return pyodbc.connect(conn_str, autocommit=db_config['autocommit'])

def test_connection(conn):
    """Prueba la conexión"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        return True
    except Exception as e:
        logging.error(f"Conexión fallida: {str(e)}")
        return False

def etl(query, source_conn, target_conn):
    """Proceso ETL para una query específica"""
    try:
        # EXTRACT
        with source_conn.cursor() as cursor:
            cursor.execute(query.extract_query)
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
        
        if not rows:
            logging.warning(f"No hay datos para: {query.extract_query[:50]}...")
            return
        
        logging.info(f"Extraídos {len(rows)} registros de: {query.extract_query[:50]}...")

        # TRANSFORM (Preparar datos para carga)
        # Convertir objetos datetime a strings si es necesario
        data_to_load = []
        for row in rows:
            processed_row = []
            for value in row:
                if isinstance(value, datetime):
                    processed_row.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                else:
                    processed_row.append(value if value is not None else None)
            data_to_load.append(processed_row)

        # LOAD
        with target_conn.cursor() as cursor:
            # Seleccionar la base de datos correcta
            cursor.execute("USE DW_CHINOOK")
            
            # Ejecutar carga con parámetros
            cursor.executemany(query.load_query, data_to_load)
            target_conn.commit()
        
        logging.info(f"Cargados {len(data_to_load)} registros")

    except pyodbc.Error as e:
        logging.error(f"Error en ETL: {str(e)}")
        logging.error(f"SQL: {query.extract_query[:200]}...")
        target_conn.rollback()
        raise

def etl_process(queries, target_config, source_config):
    """Ejecuta todo el proceso ETL"""
    try:
        # Establecer conexiones
        source_conn = create_connection(source_config)
        target_conn = create_connection(target_config)
        
        # Verificar conexiones
        if not (test_connection(source_conn) and test_connection(target_conn)):
            raise ConnectionError("Conexiones a BD fallaron")
        
        logging.info("Iniciando proceso ETL con pyodbc...")
        
        # Procesar cada query
        for query in queries:
            logging.info(f"Procesando: {query.extract_query[:50]}...")
            etl(query, source_conn, target_conn)
        
        logging.info("ETL completado exitosamente")
        
    except Exception as e:
        logging.error(f"Error en el proceso: {str(e)}")
        raise
    finally:
        # Cerrar conexiones
        if 'source_conn' in locals(): source_conn.close()
        if 'target_conn' in locals(): target_conn.close()