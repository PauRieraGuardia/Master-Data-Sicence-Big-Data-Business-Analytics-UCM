-- 1. Configuración del entorno

USE WAREHOUSE GORILLA_WH;
USE ROLE TRAINING_ROLE;
USE DATABASE UCM;
USE SCHEMA ONU;

-- 2. Obtener los datos de población actualziados a 2021 a la tabla inicial que no contiene esos datos mediante la union de las dos tablas.

SELECT COUNTRY_ID AS ID, YEAR AS "Año" 
FROM POPULATIONS
UNION 
SELECT COUNTRY_ID AS ID, YEAR AS "Año"
FROM POPULATIONS_UPDATE;

-- En caso de que los datos existieran por dupicado, hariamos uso de UNION ALL. En nuestro caso no son duplicados. SELECT COUNTRY_ID AS ID, YEAR AS "Año" 

SELECT COUNTRY_ID AS ID, YEAR AS "Año" 
FROM POPULATIONS
UNION ALL
SELECT COUNTRY_ID AS ID, YEAR AS "Año"
FROM POPULATIONS_UPDATE;

-- 4. Obtener los años comunes que aparecen en las tablas Human_development y Population

SELECT YEAR FROM HUMAN_DEVELOPMENTS
INTERSECT 
SELECT YEAR FROM POPULATIONS;

-- 5. Paises que estan en las mismas tablas

SELECT country_id FROM HUMAN_DEVELOPMENTS
INTERSECT 
SELECT country_id FROM POPULATIONS;

-- 6. Obtener los años no comunes de las tablas Human_developments y Population

-- Aquellos que estan en human_developments pero no en populations
SELECT YEAR FROM HUMAN_DEVELOPMENTS
EXCEPT 
SELECT YEAR FROM POPULATIONS;

-- Aquellos que estan en populations pero no en human_developments
SELECT YEAR FROM POPULATIONS
EXCEPT 
SELECT YEAR FROM HUMAN_DEVELOPMENTS;


-- 7. Obtener los paises no comunes de las tablas Human_developments y Population

-- Aquellos que estan en human_developments pero no en populations
SELECT COUNTRY_ID FROM HUMAN_DEVELOPMENTS
EXCEPT 
SELECT COUNTRY_ID FROM POPULATIONS;

-- Aquellos que estan en populations pero no en human_developments
SELECT COUNTRY_ID FROM POPULATIONS
EXCEPT 
SELECT COUNTRY_ID FROM HUMAN_DEVELOPMENTS;