-- 1. Definición del contexto

USE WAREHOUSE GORILLA_WH;
USE ROLE TRAINING_ROLE;
USE DATABASE UCM;
USE SCHEMA ONU;

-- 2. Obtener los datos de HDI con el nombre del pais en lugar de ID. Por lo tanto tenemos que juntar dos tablas distintas, pues el nombre esta en otra columna

SELECT COUNTRY, GENDER, YEAR, HUMAN_DEVELOPMENT_INDEX, LIFE_EXPECTANCY_AT_BIRTH, EXPECTED_YEARS_OF_SCHOOLING, MEAN_YEARS_OF_SCHOOLING, GROSS_NATIONAL_INCOME_PER_CAPITA
FROM HUMAN_DEVELOPMENTS AS h INNER JOIN COUNTRIES AS c -- Esta función nos trae coincidencias en las tablas (INNER JOIN) estamos definiendo también ambas tablas con un nombre concreto (h y c)
ON h.COUNTRY_ID=c.COUNTRY_ID; -- Le decimos por donde queremos que se unan las tablas

-- 3. Obtener los datos HDI con el nombre del pais en lugar de ID y los ordenas por año, país orden alfabético, hdi descendente 
SELECT COUNTRY, GENDER, YEAR, HUMAN_DEVELOPMENT_INDEX, LIFE_EXPECTANCY_AT_BIRTH, EXPECTED_YEARS_OF_SCHOOLING, MEAN_YEARS_OF_SCHOOLING, GROSS_NATIONAL_INCOME_PER_CAPITA
FROM HUMAN_DEVELOPMENTS AS h INNER JOIN COUNTRIES AS c 
ON h.COUNTRY_ID=c.COUNTRY_ID
ORDER BY YEAR DESC, COUNTRY ASC, HUMAN_DEVELOPMENT_INDEX DESC; -- Función para ordenar en función de una o varias variables

-- 4. Obtener los datos de HDI y de población para aquellos ID que esten en AMBAS tablas
SELECT H.COUNTRY_ID, H.YEAR, GENDER, HUMAN_DEVELOPMENT_INDEX, POPULATION
FROM HUMAN_DEVELOPMENTS AS H INNER JOIN POPULATIONS AS P
ON H.COUNTRY_ID = P.COUNTRY_ID AND H.YEAR = P.YEAR;

-- 5. Obtener los datos de HDI y de población para aquellos ID que esten en la tabla HUMAN_DEVELOPMENTS usando la función LEFT
SELECT H.COUNTRY_ID, H.YEAR, GENDER, HUMAN_DEVELOPMENT_INDEX, POPULATION
FROM HUMAN_DEVELOPMENTS AS H LEFT JOIN POPULATIONS AS P
ON H.COUNTRY_ID = P.COUNTRY_ID AND H.YEAR = P.YEAR;

-- 6. Obtener los datos de HDI y de población para aquellos ID que esten en la tabla POPULATIONS usando la función RIGHT
SELECT P.COUNTRY_ID, P.YEAR, GENDER, HUMAN_DEVELOPMENT_INDEX, POPULATION
FROM HUMAN_DEVELOPMENTS AS H RIGHT JOIN POPULATIONS AS P
ON H.COUNTRY_ID = P.COUNTRY_ID AND H.YEAR = P.YEAR;

-- 7. Obtener los datos de HDI y de población para aquellos ID que esten en CUALQUIER tabla con la opción FULL OITER JOIN
SELECT COALESCE(H.COUNTRY_ID, P.COUNTRY_ID) AS ID, COALESCE(P.YEAR, H.YEAR) AS ANYO, H.YEAR, P.YEAR, GENDER, IFNULL(HUMAN_DEVELOPMENT_INDEX,0), POPULATION -- Coalesce susittuye una en por la otra en caso de que una de las dos sea nula, la función IFNULL pondra lo que le mandemos en caso de que sea nulo
FROM HUMAN_DEVELOPMENTS AS H FULL OUTER JOIN POPULATIONS AS P
ON H.COUNTRY_ID=P.COUNTRY_ID AND H.YEAR = P.YEAR;
