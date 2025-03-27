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

