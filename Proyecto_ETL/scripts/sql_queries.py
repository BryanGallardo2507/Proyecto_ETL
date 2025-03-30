tiempo_extract = '''
SELECT
    DATENAME(WEEKDAY, I.INVOICEDATE) AS DAY_OF_WEEK,
    DATENAME(MONTH, I.INVOICEDATE) AS MONTH,
    DATEPART(QUARTER, I.INVOICEDATE) AS QUARTER,
    YEAR(I.INVOICEDATE) AS YEAR,
    SUM(I.TOTAL) AS TOTAL_SALES
FROM 
    INVOICE I
GROUP BY
    DATENAME(WEEKDAY, I.INVOICEDATE),
    DATENAME(MONTH, I.INVOICEDATE),
    DATEPART(QUARTER, I.INVOICEDATE),
    YEAR(I.INVOICEDATE)
ORDER BY
    YEAR, QUARTER, MONTH;
'''

tiempo_load = '''
INSERT INTO dim_customers (
    customer_id,
    customer_name,
    contact_name,
    address,
    city,
    postal_code,
    country
) VALUES (?, ?, ?, ?, ?, ?, ?)
'''

#########################################
empleado_extract = '''
SELECT EMPLOYEEID, FIRST_NAME, LAST_NAME 
FROM dbo.EMPLOYEE
'''

tiempo_load = '''
INSERT INTO dim_customers (
    customer_id,
    customer_name,
    contact_name,
    address,
    city,
    postal_code,
    country
) VALUES (?, ?, ?, ?, ?, ?, ?)
'''

#########################################
cliente_extract = '''
SELECT CUSTOMERID, FIRST_NAME, LAST_NAME 
FROM dbo.EMPLOYEE
'''

tiempo_load = '''
INSERT INTO dim_customers (
    customer_id,
    customer_name,
    contact_name,
    address,
    city,
    postal_code,
    country
) VALUES (?, ?, ?, ?, ?, ?, ?)
'''

# SQL Query Class
class SqlQuery:
    def _init_(self, extract_query, load_query):
        self.extract_query = extract_query
        self.load_query = load_query

# Create instances for SqlQuery class
customers_query = SqlQuery(customers_extract, customers_load)
orders_query = SqlQuery(orders_extract, orders_load)

# Store as list for iteration
sqlserver_queries = [customers_query,orders_query]