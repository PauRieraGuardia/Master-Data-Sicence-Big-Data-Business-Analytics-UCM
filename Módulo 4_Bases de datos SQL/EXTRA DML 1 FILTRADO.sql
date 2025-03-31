-- WHERE: Filtrado extra
-- LIKE

-- 1. Configuracion del entorno

USE WAREHOUSE GORILLA_WH;
USE ROLE TRAINING_ROLE;
USE DATABASE UCM;
USE SCHEMA PLANET_EXPRESS;

SHOW TABLES;

SELECT * 
FROM ORDERS;

-- 2. Obtener el orderid, el cliente, la fecha de pedido, el peso y la fecha para los planetas que comiencen por la A

SELECT ORDERID, CUSTOMERID, ORDERDATE, FREIGHT, SHIPPLANET
FROM ORDERS
WHERE SHIPPLANET LIKE 'A%'; 

-- 3. Obtener el orderid, el cliente, la fecha de pedido, el peso y la fecha de los planetas que contengan capod.

SELECT ORDERID, CUSTOMERID, ORDERDATE, FREIGHT, SHIPPLANET
FROM ORDERS
WHERE SHIPPLANET LIKE '%capod%'

-- 4. Obtener el orderid, el cliente, la fecha de pedido, el peso y la fecha de los planetas que la primera letra no la sepamos pero siga de un ph.

SELECT ORDERID, CUSTOMERID, ORDERDATE, FREIGHT, SHIPPLANET
FROM ORDERS
WHERE SHIPPLANET LIKE '_ph%'

-- 5. Obtener todos los planetas que hayan entrado en mayo

SELECT ORDERID, CUSTOMERID, ORDERDATE, FREIGHT, SHIPPLANET
FROM ORDERS
WHERE ORDERDATE LIKE '3008-05%';


-- BETWEEN EXTRA

-- 1. Obtener aquellos pedidos obtenidos en el primer trimestre

SELECT ORDERID, CUSTOMERID, ORDERDATE, FREIGHT, SHIPPLANET
FROM ORDERS
WHERE ORDERDATE BETWEEN '3008-01-01' AND '3008-03-31'
ORDER BY 3;

-- 2. Obtener todas las columnas para aquellos planetas que empiezan por A, B o C.

SELECT *
FROM ORDERS
WHERE SHIPPLANET BETWEEN 'A' AND 'D';
