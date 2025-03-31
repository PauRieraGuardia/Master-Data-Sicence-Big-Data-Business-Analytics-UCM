-- 1. Configuración del entrono 

USE WAREHOUSE GORILLA_WH;
USE ROLE TRAINING_ROLE;
USE DATABASE UCM;
USE SCHEMA PLANET_EXPRESS;


-- Hasta el momento hemos visto la siguiente forma para agrupar

SELECT CUSTOMERID, SUM(FREIGHT)
FROM ORDERS
GROUP BY CUSTOMERID;

-- 2. Obtener del cliente, order_id, y peso de cada pedido 4 columnas extra con la suma total de todos los pedidos, la suma acumulada pedido a pedido, la suma acumulada pedido a pedido para cada cliente y otra con el sumatorio del cliente.

SELECT CUSTOMERID, ORDERID, FREIGHT, 
        SUM(FREIGHT) OVER() Total_peso_tabla,
        SUM(FREIGHT) OVER(ORDER BY CUSTOMERID,ORDERID) AS Total_acumulado_pedido,
        SUM(FREIGHT) OVER(PARTITION BY CUSTOMERID ORDER BY ORDERID) AS Total_acumulado_pedido_cliente,
        SUM(FREIGHT) OVER( PARTITION BY CUSTOMERID) AS Total_acumulado_cliente      
FROM ORDERS
ORDER BY CustomerID, Total_acumulado_pedido;



