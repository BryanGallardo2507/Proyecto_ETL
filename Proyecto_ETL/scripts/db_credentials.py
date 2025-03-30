datawarehouse_name = 'DW_CHINOOK'

#Conexion a la base de datos OLAP del Data Warehouse
datawarehouse_db_config = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': 'PC_RICARDO\\SQLEXPRESS',
    'database': datawarehouse_name,
    'user': 'sa',
    'password': '123456',
    'autocommit': True,
}
#Conexion a la base de datos OLTP
sqlserver_db_config = {
        'driver': 'ODBC Driver 17 for SQL Server',
        'server': 'PC_RICARDO\\SQLEXPRESS',
        'database': 'Chinook',
        'user': 'sa',
        'password': '123456',
        'autocommit': True,
    }