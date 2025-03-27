-- 1. Definición del Contexto
USE WAREHOUSE GORILLA_WH;
USE ROLE TRAINING_ROLE;
USE DATABASE UCM;
USE SCHEMA ONU;

-- 2. Funciones escalares: que va fila a fila

-- Obtener todos los id en minuscula
SELECT DISTINCT COUNTRY_ID, LOWER(COUNTRY_ID)-- DISTINCT sirve para utlizar solo aquellas que sean distintas, LOWER para poner en minusculas las letras
FROM HUMAN_DEVELOPMENTS;

-- Obtener el cuadrado de la población 
SELECT POPULATION, POWER(POPULATION, 2) Poblacion_Cuadrado -- Estamos indicando que ponga la población al cuadrado
FROM POPULATIONS;

-- Mostrar un mensaje en función del valor de HDI: bajo (menor 0.25), medio(0.26-0.75) o alto (mayor e 0.76)
SELECT COUNTRY_ID, YEAR, GENDER,
    CASE
        WHEN HUMAN_DEVELOPMENT_INDEX <= 0.25 THEN 'Bajo'
        WHEN HUMAN_DEVELOPMENT_INDEX >0.25 AND HUMAN_DEVELOPMENT_INDEX <= 0.75 THEN 'Medio'
        WHEN HUMAN_DEVELOPMENT_INDEX > 0.75 THEN 'Mayor' 
        ELSE 'No hay registros'
    END AS Categoria_hdi
FROM HUMAN_DEVELOPMENTS;

-- Se hace lo mismo
SELECT COUNTRY_ID, YEAR, GENDER,
    CASE
        WHEN HUMAN_DEVELOPMENT_INDEX <= 0.25 THEN 'Bajo'
        WHEN HUMAN_DEVELOPMENT_INDEX <= 0.75 THEN 'Medio'
        WHEN HUMAN_DEVELOPMENT_INDEX > 0.75 THEN 'Mayor' 
        ELSE 'No hay registros'
    END AS Categoria_hdi
FROM HUMAN_DEVELOPMENTS;

-- Obtener el ID, genero, y el HDI en 2020 solo para los paises que tengan mas de 800000000 habitantes, se puede obtener de esta manera con un JOIN
SELECT H.COUNTRY_ID, GENDER, HUMAN_DEVELOPMENT_INDEX -- Cuando la columna esta en dos o mas tablas debemos especificar cual de ellas estamos seleccionando mediante el nombre_tabla.columna.
FROM HUMAN_DEVELOPMENTS AS H 
INNER JOIN POPULATIONS AS P 
ON H.COUNTRY_ID = P.COUNTRY_ID
INNER JOIN COUNTRIES AS C
ON H.COUNTRY_ID = C.COUNTRY_ID
WHERE P.POPULATION > 800000000 AND H.YEAR = 2020 AND P.YEAR = 2020;


-- O con consultas independientes
-- a) Query principal

SELECT COUNTRY_ID, GENDER YEAR, HUMAN_DEVELOPMENT_INDEX
FROM HUMAN_DEVELOPMENTS
WHERE COUNTRY_ID IN (SELECT DISTINCT COUNTRY_ID
                    FROM POPULATIONS
                    WHERE POPULATION > 800000000);

-- b) paises superpoblados (Primero se realiza esta primera query para luego introducirla en la segunda)
SELECT DISTINCT COUNTRY_ID
FROM POPULATIONS
WHERE POPULATION > 800000000; -- Nos saldrán más paises en esta query en la completa por que en la tabla de human_developments no habrá datos para todos

-- c) Guardarlo en una VIEW
CREATE OR REPLACE VIEW PAISES_SUPERPOBLADOS_PRG AS
SELECT COUNTRY_ID, GENDER YEAR, HUMAN_DEVELOPMENT_INDEX
FROM HUMAN_DEVELOPMENTS
WHERE COUNTRY_ID IN (SELECT DISTINCT COUNTRY_ID
                    FROM POPULATIONS
                    WHERE POPULATION > 800000000);

-- 3. Funciones de agregación

-- Obtención de los totales

SELECT ROUND(AVG(HUMAN_DEVELOPMENT_INDEX),2) AS PROMEDIO_HDI, -- La función round es para redondear el decimal
    STDDEV(HUMAN_DEVELOPMENT_INDEX) AS DESVIACION_HDI, -- El resto de funciones obtienen el estadistico del total de la variable seleccionada
    MEDIAN(HUMAN_DEVELOPMENT_INDEX) AS MEDIANA_HDI,
    MAX(HUMAN_DEVELOPMENT_INDEX) AS MAXIMO_HDI,
    MIN(HUMAN_DEVELOPMENT_INDEX) AS MINIMO_HDI,
    COUNT(HUMAN_DEVELOPMENT_INDEX) AS REGISTROS_HDI,
    COUNT(*) AS FILAS
    
FROM HUMAN_DEVELOPMENTS;

-- Ahora bien, quiero los totales de un año, es decir aplicando un where

SELECT ROUND(AVG(HUMAN_DEVELOPMENT_INDEX),2) AS PROMEDIO_HDI, -- La función round es para redondear el decimal
    STDDEV(HUMAN_DEVELOPMENT_INDEX) AS DESVIACION_HDI, -- El resto de funciones obtienen el estadistico del total de la variable seleccionada
    MEDIAN(HUMAN_DEVELOPMENT_INDEX) AS MEDIANA_HDI,
    MAX(HUMAN_DEVELOPMENT_INDEX) AS MAXIMO_HDI,
    MIN(HUMAN_DEVELOPMENT_INDEX) AS MINIMO_HDI,
    COUNT(HUMAN_DEVELOPMENT_INDEX) AS REGISTROS_HDI,
    COUNT(*) AS FILAS
    
FROM HUMAN_DEVELOPMENTS
WHERE YEAR = 2021;

-- El promedio de HDI en cada uno de los años, debemos utilizar la función groupby

SELECT YEAR, AVG(HUMAN_DEVELOPMENT_INDEX) AS PROMEDIO_HDI
FROM HUMAN_DEVELOPMENTS
GROUP BY year;

-- El promedio de HDI por país y año 

SELECT YEAR, COUNTRY_ID, AVG(HUMAN_DEVELOPMENT_INDEX) AS PROMEDIO_HDI
FROM HUMAN_DEVELOPMENTS
GROUP BY year, COUNTRY_ID;

-- El promedio de HDI por pais y año del S.XXI

SELECT YEAR, COUNTRY_ID, AVG(HUMAN_DEVELOPMENT_INDEX) AS PROMEDIO_HDI
FROM HUMAN_DEVELOPMENTS
WHERE YEAR >= 2001
GROUP BY year, COUNTRY_ID;

-- el promedio de HDI por pais, año del S.XXI y los valores de HDI promedio >0.60
SELECT YEAR, COUNTRY_ID, AVG(HUMAN_DEVELOPMENT_INDEX) AS PROMEDIO_HDI
FROM HUMAN_DEVELOPMENTS
WHERE YEAR >= 2001
GROUP BY year, COUNTRY_ID
having promedio_hdi >0.6 -- El having hace la mismoa función que el where pero para las funciones de agregación.
order by promedio_hdi ;