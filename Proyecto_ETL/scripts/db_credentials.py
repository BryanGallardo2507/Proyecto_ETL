#Conexion a la base de datos OLAP del Data Warehouse
datawarehouse_db_config = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': 'PC_RICARDO\SQLEXPRESS',
    'database': 'DW_CHINOOK',
    'user': 'ADMIN',
    'password': '123456',
    'autocommit': True,
}

sqlserver_db_config = {
        'driver': 'ODBC Driver 17 for SQL Server',
        'server': 'PC_RICARDO\SQLEXPRESS',
        'database': 'Chinook',
        'user': 'ADMIN',
        'password': '123456',
        'autocommit': True,
    }