-- SMART DESK es una empresa global dedicada a la fabricación y distribución de mobiliario de oficina, especializada en soluciones ergonómicas y tecnológicamente avanzadas. En los últimos años, SMART DESK ha expandido sus operaciones a nivel global, lo que ha generado la necesidad de analizar datos para ajustar sus estrategias de ventas, optimziar sus pronósticos y maximizar el benefciio en sus operaciones.

-- 1. Definimos el contexto
-- Vamos a crear un nuevo scema para usarlo dentro de nuestro warehouse GORI

USE WAREHOUSE GORILLA_WH;
USE ROLE TRAINING_ROLE;
USE DATABASE GORILLA_DB;
CREATE SCHEMA SMART_DESK;
USE SCHEMA SMART_DESK;

-- Una vez hemos ejecutado el entorno donde vamos a trabajar, debemos añadir los datos al schema SMART_DESK. Este proceso se realzia desde el apartado data.

SHOW TABLES;

-- Podemos osbervar las tres tablas de datos, FORECASTS, ACCOUNTS y SALES. 
-- La tabla Sales contiene datos de ventas reales por cuenta, categoria de producto, año, trimestre y unidades vendidas.
-- La tabla Accounts contiene dealles sobre las cuentas de clientes, como su ubicación, industria y los contactos relevantes.
-- La tabla Forecasts proporciona pronósticos de beneficios y oportunidades comerciales futuras.
-- Vamos a echar un ojo al contenido de las tablas

SELECT *
FROM SALES;
-- Contiene 159 ventas, donde de cada una se especifica el identificador de la cuenta, la cateogria de venta, el trimestre y año, el ingreso generado por el mantenimiento, ingreso generado por la venta de partes, ingreso generado por la venta de productos principales, el beneficio de la venta (deduciendo costes), ingreso por servicios de soporte, el total (que es la suma de mantenimiento, partes, producto y soporte) y las unidades vendidas.

SELECT * 
FROM ACCOUNTS;
-- Contiene 188 cuentas, donde de cada una indica el identificador unico de la cuenta, el nombre del ejecutivo de cuentas que gestiona la relación con el cliente, la clasificación de la cuenta en función de su importancia, la diercción de correo electrónico del contacto principal de la cuenta, el país, la industria y la región.

SELECT *
FROM FORECASTS;
-- Contiene 135 variables donde de cada una se proporciona el identificador de la cuenta o cliente, la categoria de productos o servicios pronosticados, la clasificación del pronóstico (Pipeline, Best Case, Commited), el valor en dólares proyectado como beneficio futuro, la edad de oportunidad en días, y el año pronosticado

-- EJERCICIO 1: Realzia un análisis de las ventas y el beneficio total desglosado por cateogoría de producto exclusivamente para la cuenta Adabs Entertainment durante el año 2020. 

SELECT CATEGORY AS CATEGORIA, SUM(MAINTENANCE) AS MANTENIMIENTO , 
        SUM(PRODUCT) AS PRODUCTO , SUM(PARTS) AS PARTES, SUM(SUPPORT) AS SOPORTE,
        SUM(MAINTENANCE)+SUM(PRODUCT)+SUM(PARTS)+SUM(SUPPORT) AS VENTAS, 
        SUM(UNITS_SOLD) AS UNIDADES_VENDIDAS, SUM(PROFIT) AS BENEFICIO_TOTAL
FROM SALES
WHERE ACCOUNT = 'Adabs Entertainment' AND YEAR = 2020
GROUP BY CATEGORIA;
-- Podemos osbervar que en 2020 se vendieron dos categorias de productos, sillas y electronicos, con unas ventas de 1835672€ para las sillas y de 2100000€ para los electronics con un total de unidades vendidas de 3530.49 para el primero y 3571.43 para el segundo reportando un beneficio de 605.772€ para las sillas y de 756.000€ para los electronicos. 

-- EJERCICIO 2: Comparación de Ventas, unidades vendidas y beneficio entre países en las regiones APAC y EMEA
SELECT COUNTRY AS PAIS, ROUND(AVG(TOTAL), 2) AS VENTAS, 
        ROUND(AVG(UNITS_SOLD), 2) AS UNIDADES_VENDIDAS_PROMEDIO, 
        ROUND(AVG(PROFIT),2) AS BENEFICIOS_PROMEDIOS
FROM SALES AS S JOIN ACCOUNTS AS A
ON S.ACCOUNT = A.ACCOUNT
WHERE REGION = 'APAC' OR REGION = 'EMEA'
GROUP BY PAIS;
-- Podemos observar el promedio de ingreso de beneficios y de unidades vendidas por año para los 12 países pertenecientes a las regiones APAC y EMEA

-- EJERCICIO 3: Análisis del Beneficio total por industria: Estudio de Clientes en Etapa de Compromiso
SELECT INDUSTRY AS INDUSTRIA, SUM(PROFIT) AS BENEFICIO_TOTAL, 
    CASE 
        WHEN SUM(PROFIT) > 1000000 THEN 'ALTO'
        ELSE 'NORMAL'
    END AS CATEGORIA
FROM SALES AS S JOIN ACCOUNTS AS A
ON S.ACCOUNT = A.ACCOUNT
WHERE A.ACCOUNT IN (
    SELECT ACCOUNT
    FROM FORECASTS 
    WHERE PREDICTION_CATEGORY = 'Commit'
    GROUP BY ACCOUNT
    HAVING SUM(FORECAST) > 500000
)
GROUP BY INDUSTRIA;

-- Como podemos observar, solo hay una industria que tenga menos de 1000000 de beneficio total y mas de 500000.

-- EJERCICIO 4: Evolución del Pronóstico y Beneficio Real. Análisis de la Trayectoria por categoría. Calcula el pronóstico de beneficio para el año 2022 y el beneficio real para el primer trimestre de 2020 y el tercer trimestre de 2021, agrupando los resultados por categoría de producto. Además, queremos identificar la oportunidad más antigua y la más reciente dentro de cada categoria.
SELECT COALESCE(S.CATEGORY,F.CATEGORY) AS CATEGORIA, 
    SUM(CASE WHEN F.YEAR = 2022 THEN F.FORECAST ELSE 0 END) AS PRONOSTICO_2022,
    SUM(CASE WHEN QUARTER = '2020 Q1' THEN S.PROFIT ELSE 0 END) AS BENEFICIO_2020_Q1,
    SUM(CASE WHEN QUARTER = '2021 Q3' THEN S.PROFIT ELSE 0 END) AS BENEFICIO_2021_Q3,
    MAX(OPPORTUNITY_AGE) AS OPORTUNIDAD_MAS_ANTIGUA,
    MIN(OPPORTUNITY_AGE) AS OPORTUNIDAD_MAS_RECIENTE
FROM SALES AS S FULL OUTER JOIN FORECASTS AS F
ON S.CATEGORY = F.CATEGORY AND S.YEAR = F.YEAR 
GROUP BY CATEGORIA
ORDER BY CATEGORIA;

-- EJERCICIO 5: Caso práctico: Análisis libre

-- El objetivo del estudio es realizar una comparación entre las distintas cuentas para encontrar aquellas que llevan creciendo en beneficio, en ventas y en unidades vendidas para cada año


  SELECT 
  S.ACCOUNT AS CUENTA,
  A.COUNTRY AS PAIS,
  A.INDUSTRY AS INDUSTRIA,
  SUM(CASE WHEN S.YEAR = '2019' THEN S.PROFIT ELSE 0 END) AS BENEFICIO_2019,
  SUM(CASE WHEN S.YEAR = '2020' THEN S.PROFIT ELSE 0 END) AS BENEFICIO_2020,
  SUM(CASE WHEN S.YEAR = '2021' THEN S.PROFIT ELSE 0 END) AS BENEFICIO_2021,
  SUM(CASE WHEN S.YEAR = '2019' THEN S.TOTAL ELSE 0 END) AS VENTAS_2019,
  SUM(CASE WHEN S.YEAR = '2020' THEN S.TOTAL ELSE 0 END) AS VENTAS_2020,
  SUM(CASE WHEN S.YEAR = '2021' THEN S.TOTAL ELSE 0 END) AS VENTAS_2021,
  SUM(CASE WHEN S.YEAR = '2019' THEN S.UNITS_SOLD ELSE 0 END) AS UNIDADES_VENDIDAS_2019,
  SUM(CASE WHEN S.YEAR = '2020' THEN S.UNITS_SOLD ELSE 0 END) AS UNIDADES_VENDIDAS_2020,
  SUM(CASE WHEN S.YEAR = '2021' THEN S.UNITS_SOLD ELSE 0 END) AS UNIDADES_VENDIDAS_2021,
  
  CASE
    WHEN 
        SUM(CASE WHEN S.YEAR = '2019' THEN S.PROFIT ELSE 0 END) <
        SUM(CASE WHEN S.YEAR = '2020' THEN S.PROFIT ELSE 0 END)
        AND
        SUM(CASE WHEN S.YEAR = '2020' THEN S.PROFIT ELSE 0 END) <
        SUM(CASE WHEN S.YEAR = '2021' THEN S.PROFIT ELSE 0 END)
    THEN 'POTENCIAL_CRECIMIENTO'
    ELSE 'NO_HAY_CRECIMIENTO' 
    END AS AUMENTO_BENEFICIO ,


  CASE
    WHEN 
        SUM(CASE WHEN S.YEAR = '2019' THEN S.TOTAL ELSE 0 END) <
        SUM(CASE WHEN S.YEAR = '2020' THEN S.TOTAL ELSE 0 END)
        AND
        SUM(CASE WHEN S.YEAR = '2020' THEN S.TOTAL ELSE 0 END) <
        SUM(CASE WHEN S.YEAR = '2021' THEN S.TOTAL ELSE 0 END)
    THEN 'POTENCIAL_CRECIMIENTO'
    ELSE 'NO_HAY_CRECIMIENTO' 
    END AS AUMENTO_VENTAS ,
    
  CASE
    WHEN 
        SUM(CASE WHEN S.YEAR = '2019' THEN S.UNITS_SOLD ELSE 0 END) <
        SUM(CASE WHEN S.YEAR = '2020' THEN S.UNITS_SOLD ELSE 0 END)
        AND
        SUM(CASE WHEN S.YEAR = '2020' THEN S.UNITS_SOLD ELSE 0 END) <
        SUM(CASE WHEN S.YEAR = '2021' THEN S.UNITS_SOLD ELSE 0 END)
    THEN 'POTENCIAL_CRECIMIENTO'
    ELSE 'NO_HAY_CRECIMIENTO' 
    END AS AUMENTO_UNIDADES_VENDIDAS ,

FROM SALES AS S
JOIN ACCOUNTS AS A ON S.ACCOUNT = A.ACCOUNT
GROUP BY S.ACCOUNT, A.COUNTRY, A.INDUSTRY
ORDER BY S.ACCOUNT;


-- Creamos una view con aquellos datos que queremos utilizar y que encontramos en las tablas SALES y ACCOUNTS. Es decir, la cuenta, el país, la indústria, el beneficio, las ventas, y las unidades_vendidas para cada año. 
-- Este código nos muestra tanto si se cumple el aumento de ventas, de unidades vendidas y beneficios en los años 2019, 2020, y 2021 como los importes de dichas categorias por año.
-- Si queremos simplificar el codigo y solamente mostrar si se cumple la condición debemos runear el siguiente codigo

SELECT 
  S.ACCOUNT AS CUENTA,
  A.COUNTRY AS PAIS,
  A.INDUSTRY AS INDUSTRIA,
  
  CASE
    WHEN 
        SUM(CASE WHEN S.YEAR = '2019' THEN S.PROFIT ELSE 0 END) <
        SUM(CASE WHEN S.YEAR = '2020' THEN S.PROFIT ELSE 0 END)
        AND
        SUM(CASE WHEN S.YEAR = '2020' THEN S.PROFIT ELSE 0 END) <
        SUM(CASE WHEN S.YEAR = '2021' THEN S.PROFIT ELSE 0 END)
    THEN 'POTENCIAL_CRECIMIENTO'
    ELSE 'NO_HAY_CRECIMIENTO' 
    END AS AUMENTO_BENEFICIO ,

  CASE
    WHEN 
        SUM(CASE WHEN S.YEAR = '2019' THEN S.TOTAL ELSE 0 END) <
        SUM(CASE WHEN S.YEAR = '2020' THEN S.TOTAL ELSE 0 END)
        AND
        SUM(CASE WHEN S.YEAR = '2020' THEN S.TOTAL ELSE 0 END) <
        SUM(CASE WHEN S.YEAR = '2021' THEN S.TOTAL ELSE 0 END)
    THEN 'POTENCIAL_CRECIMIENTO'
    ELSE 'NO_HAY_CRECIMIENTO' 
    END AS AUMENTO_VENTAS ,
    
  CASE
    WHEN 
        SUM(CASE WHEN S.YEAR = '2019' THEN S.UNITS_SOLD ELSE 0 END) <
        SUM(CASE WHEN S.YEAR = '2020' THEN S.UNITS_SOLD ELSE 0 END)
        AND
        SUM(CASE WHEN S.YEAR = '2020' THEN S.UNITS_SOLD ELSE 0 END) <
        SUM(CASE WHEN S.YEAR = '2021' THEN S.UNITS_SOLD ELSE 0 END)
    THEN 'POTENCIAL_CRECIMIENTO'
    ELSE 'NO_HAY_CRECIMIENTO' 
    END AS AUMENTO_UNIDADES_VENDIDAS ,

FROM SALES AS S
JOIN ACCOUNTS AS A ON S.ACCOUNT = A.ACCOUNT
GROUP BY S.ACCOUNT, A.COUNTRY, A.INDUSTRY
ORDER BY AUMENTO_BENEFICIO DESC; 

-- En este código, si bien es cierto que estará más simplificado, no veremos donde no se da el aumento, si en el año 2019 o en el 2020. Por lo tanto, estará más simplificado pero tendrá un análisis menos concreto.















