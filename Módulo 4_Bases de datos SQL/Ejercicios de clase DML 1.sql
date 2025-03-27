-- 1. Definimos el contexto

USE WAREHOUSE GORILLA_WH;
USE ROLE TRAINING_ROLE;
USE DATABASE UCM; -- No es el mismo rol pues no hemos seleccionado la data base GORILA_db, sino la UCM
USE SCHEMA ONU;

-- 2. Exploración inicial de la base de datos

SHOW TABLES;

SELECT *
FROM HUMAN_DEVELOPMENTS; -- Podemos ejecutar la tabla human_developments por que se encuentra en el contexto definido, no se habrán realizado los cambios realizados en otro contexto

SELECT * 
FROM POPULATIONS;

SELECT * 
FROM COUNTRIES;

SELECT *
FROM PAID_DAYS_OFF;

-- 3. Preguntas de análisis

-- Seleccionar el ID, el Ño y la población de la tabla population
SELECT COUNTRY_ID, YEAR, POPULATION -- Seleccionamos las columnas que queremos
FROM POPULATIONS; -- Seleccionamos la tabla 

-- Seleccionar el ID, el año y la población pero con nombres en español 
SELECT COUNTRY_ID AS Id_Pais, YEAR AS "Año", POPULATION as Poblacion
FROM POPULATIONS;

-- Seleccionar el ID, el año y la población pero con nombres en español para españa
SELECT COUNTRY_ID AS Id_pais, YEAR AS "AÑO", POPULATION AS Poblacion
FROM POPULATIONS
WHERE country_id = 'ESP'; 

-- Los datos (PAIS, AÑO, GENERO) de HDI a partir del año 2000
SELECT COUNTRY_ID, YEAR, GENDER
FROM HUMAN_DEVELOPMENTS
WHERE YEAR > 1999;

-- Se puede coneguir de otra manera
SELECT COUNTRY_ID, YEAR, GENDER
FROM HUMAN_DEVELOPMENTS
WHERE YEAR >= 2000;

-- Obtener los datos de la esperanza de vida 
SELECT COUNTRY_ID, YEAR, LIFE_EXPECTANCY_AT_BIRTH AS Esperanza_vida_mujeres
FROM HUMAN_DEVELOPMENTS
WHERE GENDER = 'Female';

-- De otra forma
SELECT COUNTRY_ID, YEAR, LIFE_EXPECTANCY_AT_BIRTH AS Esperanza_vida_mujeres
FROM HUMAN_DEVELOPMENTS
WHERE GENDER != 'Male';

-- Seleccionar varias condiciones
-- La renta per capita de hombres españoles en el 2000
SELECT COUNTRY_ID,  GROSS_NATIONAL_INCOME_PER_CAPITA
FROM HUMAN_DEVELOPMENTS
WHERE (COUNTRY_ID = 'ESP') AND (GENDER= 'Male') AND (YEAR >= 2000);

-- Escolarización de mujeres francesas y hombres italianos en 2020
SELECT COUNTRY_ID, YEAR, GENDER, mean_years_of_schooling
FROM HUMAN_DEVELOPMENTS
WHERE ((COUNTRY_ID = 'FRA' AND GENDER = 'Female') OR (COUNTRY_ID ='ITA' AND GENDER = 'Male')) AND YEAR=2020; -- Los parentesis sirven para ir separando las distintas condiciones

-- Obtener todas las columnas de la decada de los 90
SELECT * 
FROM HUMAN_DEVELOPMENTS
WHERE YEAR >= 1990 AND YEAR <2000;

-- Podemos obtenerlo de otra forma de obtenerlo
SELECT * 
FROM HUMAN_DEVELOPMENTS
WHERE year BETWEEN 1990 AND 1999;

-- Podemos utilizarlo también para letras, la diferencia es que en las letras no pilla la última letra
SELECT * 
FROM HUMAN_DEVELOPMENTS
WHERE COUNTRY_ID BETWEEN 'A' AND 'B';

-- Obtener los paises que empiezan por X, Y y Z
SELECT *
FROM HUMAN_DEVELOPMENTS
WHERE COUNTRY_ID >= 'X';

-- El operador % nos muestra todo lo que siga despues de la letra
SELECT *
FROM HUMAN_DEVELOPMENTS
WHERE COUNTRY_ID LIKE 'X%' OR COUNTRY_ID LIKE 'Y%' OR COUNTRY_ID LIKE 'Z%'; 

-- Obtener toda la información de los paises del G7
SELECT * 
FROM HUMAN_DEVELOPMENTS
WHERE COUNTRY_ID = 'USA' OR COUNTRY_ID = 'CAN' OR COUNTRY_ID = 'ITA' OR COUNTRY_ID = 'FRA' OR COUNTRY_ID = 'DEU' OR COUNTRY_ID = 'GBR' OR COUNTRY_ID = 'JPN';

-- Otra forma de obtenerlo es mediante el operador IN
SELECT *
FROM HUMAN_DEVELOPMENTS
WHERE COUNTRY_ID IN('USA','CAN','ITA','FRA','DEU','GBR','JPN');

-- Obtener los datos de esperanza de vida de mujeres
SELECT COUNTRY_ID, YEAR, LIFE_EXPECTANCY_AT_BIRTH AS Esperanza_vida_muejers
FROM HUMAN_DEVELOPMENTS
WHERE NOT GENDER= 'Male';

-- Obtener todos los países que NO son del G7
SELECT *
FROM HUMAN_DEVELOPMENTS
WHERE COUNTRY_ID NOT IN('USA','CAN','ITA','FRA','DEU','GBR','JPN');

-- Identificar los paises con valores HDI nulos
SELECT COUNTRY_ID, YEAR, GENDER, HUMAN_DEVELOPMENT_INDEX
FROM HUMAN_DEVELOPMENTS
WHERE HUMAN_DEVELOPMENT_INDEX IS NULL;

-- Traer los datos de HUMAN DEVELOPMENTS sin nulos
SELECT COUNTRY_ID, YEAR, GENDER, HUMAN_DEVELOPMENT_INDEX
FROM HUMAN_DEVELOPMENTS
WHERE HUMAN_DEVELOPMENT_INDEX IS NOT NULL;

-- Guardar en una lista la tabla HDI sin nulos
CREATE VIEW HUMAN_DEVELOPMENTS_SIN_NULOS_PRG AS -- Estamos creando una vista, donde posteriormente adjuntamos la query de la lista que queremos guardar
SELECT *
FROM HUMAN_DEVELOPMENTS
WHERE HUMAN_DEVELOPMENT_INDEX IS NOT NULL;
-- Este proceso es como tomar una foto a la data, lo que es dinámica porque se irá remplazando en caso de que cambie la query. 

-- Visualizamos los datos que hemos guardaro en nuestra vista
SELECT *
FROM HUMAN_DEVELOPMENTS_SIN_NULOS_PRG;