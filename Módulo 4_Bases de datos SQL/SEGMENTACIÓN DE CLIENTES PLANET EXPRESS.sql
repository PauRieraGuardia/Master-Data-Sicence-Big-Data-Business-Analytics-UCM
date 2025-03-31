-- Vamos a realizar la práctica de clase relacionada con la segmentación de clientes para Planet Express
-- Esta segmentación se dara en base a la frecuencia de pedidos y el peso de los envíos. 
-- Evidentemente, Planet Express es una empresa fictica 

-- 1. Definición del entorno

USE WAREHOUSE GORILLA_WH;
USE ROLE TRAINING_ROLE;
USE DATABASE UCM;
USE SCHEMA PLANET_EXPRESS;

-- 2. Análisis exploratorio 

SELECT COUNT(ORDERID) AS Num_Pedidos, 
        COUNT(*) AS Num_Filas, 
        COUNT(DISTINCT CUSTOMERID) AS Num_Clientes, 
        ROUND(AVG(FREIGHT),2) AS Promedio_Peso,
        ROUND(STDDEV(FREIGHT),2) AS Desviacion_Peso,
        MAX(FREIGHT) AS Maximo_Peso,
        MIN(FREIGHT) AS Minimo_Peso,
        MIN(ORDERDATE) AS Pedido_mas_antiguo,
        MAX(ORDERDATE) AS Pedido_mas_reciente,
FROM ORDERS;

-- Obtenemos un número de pedidos de 829, 89 clientes distintos un peso promedio de los envios de 78,52kg con una desviación standard de 111,66 kg, lo que indica una alta variabilidad en el peso de los pedidos. El rango de pedidos varia desde el 3006/07/04 a 3008/05/06.

-- 3. Desarrollo del análisis

-- Vamos a crear una vista que nos permita segmentar los clientes en funcion de los pedidos por dia 

CREATE OR REPLACE VIEW Pedidios_dia AS 
SELECT CUSTOMERID, ORDERDATE, COUNT(*) AS Pedidos_dia
FROM ORDERS
GROUP BY CUSTOMERID,ORDERDATE;

SELECT *
FROM PEDIDIOS_DIA;

-- Vamos a utilziar la vista creada para calcular el promedio de pedidos por dia por cliente

SELECT CUSTOMERID, ROUND(AVG(PEDIDOS_DIA),2) AS Promedio_pedidos_dia_cliente 
FROM PEDIDOS_DIA
GROUP BY CUSTOMERID
ORDER BY Promedio_pedidos_dia_cliente DESC;

-- Podemos observar que, salvo 6 clientes, el resto realizan de promedio un pedido diario

-- Vamos a crear una vista para agrupar los pedidos por cliente y calcular el peso máximos de los envios realizados por cada uno.

CREATE OR REPLACE VIEW Peso_Cliente AS
SELECT CUSTOMERID, MAX(FREIGHT) AS PESO_MAXIMO_ENVIO
FROM ORDERS
GROUP BY CUSTOMERID;

SELECT *
FROM PESO_CLIENTE;


-- 3. Criterios de Segmentación 

-- Establecemos la siguiente segmentación en base a los resultados obtenidos.
-- Cliente GOLD: Más de un pedido por dia y un peso máximo de envio meno a 195kg
-- Cliente SILVER: Más de un pedido por día pero con un peso máximo superior a 195kg
-- Cliente Standard: Menos de un pedodo por día, independientemente del peso

SELECT S1.CUSTOMERID, ROUND(AVG(PEDIDOS_DIA),2) AS PROMEDIO_PEDIDOS_DIA_CLIENTE, MAX(S2.PESO_MAXIMO_ENVIO) AS PESO_MAXIMO_CLIENTE,
CASE
    WHEN PROMEDIO_PEDIDOS_DIA_CLIENTE >1 AND PESO_MAXIMO_CLIENTE < 195 THEN 'Cliente Gold'
    WHEN PROMEDIO_PEDIDOS_DIA_CLIENTE >1 AND PESO_MAXIMO_CLIENTE > 195 THEN 'Cliente Silver'
    ELSE 'Cliente Standard'
END AS TIPO_CLIENTE
FROM PEDIDIOS_DIA AS S1 JOIN PESO_CLIENTE AS S2 
ON S1.CUSTOMERID = S2.CUSTOMERID
GROUP BY S1.CUSTOMERID;

-- Vamos a guardarlo como vista para poder consultarlo cuando queramos

CREATE OR REPLACE VIEW SEGMENTACION_CLIENTES AS
SELECT S1.CUSTOMERID, ROUND(AVG(PEDIDOS_DIA),2) AS PROMEDIO_PEDIDOS_DIA_CLIENTE, MAX(S2.PESO_MAXIMO_ENVIO) AS PESO_MAXIMO_CLIENTE,
CASE
    WHEN PROMEDIO_PEDIDOS_DIA_CLIENTE >1 AND PESO_MAXIMO_CLIENTE < 195 THEN 'Cliente Gold'
    WHEN PROMEDIO_PEDIDOS_DIA_CLIENTE >1 AND PESO_MAXIMO_CLIENTE > 195 THEN 'Cliente Silver'
    ELSE 'Cliente Standard'
END AS TIPO_CLIENTE
FROM PEDIDIOS_DIA AS S1 JOIN PESO_CLIENTE AS S2 
ON S1.CUSTOMERID = S2.CUSTOMERID
GROUP BY S1.CUSTOMERID;

SELECT*
FROM SEGMENTACION_CLIENTES;