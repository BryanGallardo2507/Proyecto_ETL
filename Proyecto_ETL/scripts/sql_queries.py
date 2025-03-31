#QUERIES DE TIEMPO
tiempo_extract = '''
SELECT
    CONVERT(DATE, InvoiceDate) AS TIEMPO_ID,
    DATENAME(MONTH, I.INVOICEDATE) AS MES,
    DATEPART(QUARTER, I.INVOICEDATE) AS TRIMESTRE,
    YEAR(I.INVOICEDATE) AS ANIO,
    DATENAME(WEEKDAY, I.INVOICEDATE) AS DIA_DE_SEMANA
FROM 
    DBO.INVOICE I
ORDER BY
    ANIO, TRIMESTRE, MES;

'''

tiempo_load = '''
INSERT INTO DIMENSION_TIEMPO (
    TIEMPO_ID,
    MES,
    TRIMESTRE,
    ANIO,
    DIA_SEMANA
) VALUES (?, ?, ?, ?, ?)
'''

#QUERIES DE EMPLEADOS
empleados_extract = '''
SELECT 
    EMPLOYEEID,
    FIRSTNAME,
    LASTNAME
FROM DBO.EMPLOYEE;
'''

empleados_load = '''
INSERT INTO DIMENSION_EMPLEADO (
    EMPLEADOID,
    NOMBRE,
    APELLIDO
) VALUES (?, ?, ?)
'''

#QUERIES DE CLIENTES
cliente_extract = '''
SELECT 
    CUSTOMERID,
    FIRSTNAME,
    LASTNAME,
    ADDRESS,
    CITY,
    STATE,
    COUNTRY
FROM DBO.CUSTOMER;
'''

cliente_load = '''
INSERT INTO DIMENSION_CLIENTE (
    CLIENTEID,
    NOMBRE,
    APELLIDO,
    DIRECCION,
    CIUDAD,
    ESTADO,
    PAIS
) VALUES (?, ?, ?, ?, ?, ?, ?)
'''

#QUERIES DE PISTAS
pistas_extract = '''
SELECT 
    TRACKID,
    NAME,
    GENREID,
    MEDIATYPEID,
    ALBUMID
FROM DBO.TRACK;
'''

pistas_load = '''
INSERT INTO DIMENSION_PISTAS (
    PISTAID,
    NOMBRE,
    GENEROID,
    MEDIOID,
    ALBUMID
) VALUES (?, ?, ?, ?, ?)
'''


#QUERIES DE TIPO MEDIO
tipo_medio_extract = '''
    SELECT 
        MEDIATYPEID,
        NAME
    FROM DBO.MEDIATYPE;
'''

tipo_medio_load = '''
INSERT INTO DIMENSION_MEDIOTIPO (
    MEDIOID,
    NOMBRE
) VALUES (?, ?)
'''

#QUERIES DE GENERO
genero_extract = '''
    SELECT 
        GENREID,
        NAME
    FROM DBO.GENRE;
'''

genero_load = '''
INSERT INTO DIMENSION_GENERO (
    GENEROID,
    NOMBRE
) VALUES (?, ?)
'''

#QUERIES DE ALBUM
album_extract = '''
    SELECT 
        ALBUMID,
        TITLE,
        ARTISTID
    FROM DBO.ALBUM;
'''

album_load = '''
INSERT INTO DIMENSION_ALBUM (
    ALBUMID,
    TITULO,
    ARTISTAID
) VALUES (?, ?, ?)
'''

#QUERIES DE ARTISTAS
artista_extract = '''
SELECT
    ARTISTID,
    NAME
FROM DBO.ARTIST 
'''

artista_load = '''
INSERT INTO DIMENSION_ARTISTA (
    ARTISTAID,
    NOMBRE
) VALUES (?, ?)
'''

#QUERIES DE TABLA DE HECHOS
hechos_extract = '''
SELECT 	
	CONVERT(DATE, B.INVOICEDATE) AS TIEMPO_ID,
	C.CUSTOMERID,
	D.TRACKID,
	E.EMPLOYEEID,
	F.MEDIATYPEID,
	G.GENREID,
	H.ALBUMID,
	I.ARTISTID,
	SUM(A.UNITPRICE*A.QUANTITY) AS VENTAS
FROM INVOICELINE A
INNER JOIN INVOICE B
ON A.INVOICEID=B.INVOICEID
INNER JOIN CUSTOMER C
ON B.CUSTOMERID=C.CUSTOMERID
INNER JOIN TRACK D
ON A.TRACKID=D.TRACKID
INNER JOIN EMPLOYEE E
ON C.[SupportRepId]=E.EmployeeId
INNER JOIN MEDIATYPE F
ON D.MEDIATYPEID=F.MEDIATYPEID
INNER JOIN GENRE G
ON D.GENREID=G.GENREID
INNER JOIN ALBUM H
ON D.ALBUMID=H.ALBUMID
INNER JOIN ARTIST I
ON H.ARTISTID=I.ARTISTID
GROUP BY A.INVOICELINEID,
	CONVERT(DATE, B.INVOICEDATE),
	C.CUSTOMERID,
	D.TRACKID,
	E.EMPLOYEEID,
	F.MEDIATYPEID,
	G.GENREID,
	H.ALBUMID,
	I.ARTISTID
;
'''

hechos_load = '''
INSERT INTO TBL_HECHOS (
    TIEMPO_ID,
    EMPLEADOID,
    CLIENTEID,
    PISTAID,
    MEDIOID,
    GENEROID,
    ALBUMID,
    ARTISTAID,
    VENTAS
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

# SQL Query Class
class SqlQuery:
    def __init__(self, extract_query, load_query):
        self.extract_query = extract_query
        self.load_query = load_query

# Create instances for SqlQuery class
tiempo_query = SqlQuery(tiempo_extract, tiempo_load)
empleados_query = SqlQuery(empleados_extract, empleados_load)
cliente_query = SqlQuery(cliente_extract, cliente_load)
pistas_query = SqlQuery(pistas_extract, pistas_load)
tipo_medio_query = SqlQuery(tipo_medio_extract, tipo_medio_load)
genero_query = SqlQuery(genero_extract, genero_load)
album_query = SqlQuery(album_extract, album_load)
artista_query = SqlQuery(artista_extract, artista_load)
hechos_query = SqlQuery(hechos_extract, hechos_load)

# Store as list for iteration
sqlserver_queries = [
    tiempo_query,  
    artista_query, 
    album_query,
    genero_query,
    tipo_medio_query,
    pistas_query,
    empleados_query,
    cliente_query,
    hechos_query       
]