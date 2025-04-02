# Configuración de la conexión a la base de datos
source_config = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': 'bryan-pc\SQLEXPRESS',
    'database': 'Chinook',
    'user': 'sa',
    'password': '123456',
    'port': 1433,
    'autocommit': True,
}

target_config = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': 'bryan-pc\SQLEXPRESS',
    'database': 'DW_Chinook',
    'user': 'sa',
    'password': '123456',
    'port': 1433,
    'autocommit': True,
}
