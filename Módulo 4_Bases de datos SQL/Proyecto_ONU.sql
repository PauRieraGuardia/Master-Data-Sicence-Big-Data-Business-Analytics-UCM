-- Vamos a realizar un proyecto con una database de la ONU.
-- En esta practica asumiremos el papel de analistas de datos para la ONU
-- Esta centrada en el análisis del desarrollo humano en diferentes naciones. 
-- Encontramos los siguientes conjuntos de datos distintos, human_development, country_population, country_population_2021, paid_days_off, countries

-- Vayamos a confiugrar el entorno de ejecución
USE WAREHOUSE GORILLA_WH;
USE ROLE TRAINING_ROLE;
USE DATABASE UCM;
USE SCHEMA ONU;

-- 1. SQL Queries I: Keywords y funciones (SELECT Y FROM)

-- Mostrar todas las filas y columnas de las cinco tablas

SHOW TABLES;

SELECT *
FROM COUNTRIES;
SELECT *
FROM HUMAN_DEVELOPMENTS;
SELECT *
FROM PAID_DAYS_OFF;
SELECT *
FROM POPULATIONS;
SELECT *
FROM POPULATIONS_UPDATE;

-- Mostrar la columna country_id, year, gender, y esperanza de vida al nacer

SELECT COUNTRY_ID, YEAR, GENDER, LIFE_EXPECTANCY_AT_BIRTH
FROM HUMAN_DEVELOPMENTS;

-- Mostrar distintos country id de la tabla de poblaciones (1 vez cada country_id)

SELECT DISTINCT COUNTRY_ID 
FROM HUMAN_DEVELOPMENTS;

-- Mostrar el country id y población. Crear un alias y llamarlo población

SELECT COUNTRY_ID, POPULATION AS POBLACION
FROM POPULATIONS;

-- 2. SQL Queries I: Keywords y funciones básicas (WHERE)

-- Mostrar toda la información de la tabla índice de desarrollo humano para España 

SELECT *
FROM HUMAN_DEVELOPMENTS
WHERE COUNTRY_ID = 'ESP';

-- Mostrar los datos de la población de españa (ESP) después del 2010

SELECT *
FROM POPULATIONS
WHERE COUNTRY_ID = 'ESP' AND YEAR >= 2010;

-- Mostrar todos los datos de la tabla de días libres para aquellos países que tengan más de 10 festivos nacionales

SELECT COUNTRY_ID, PAID_PUBLIC_HOLIDAYS
FROM PAID_DAYS_OFF
WHERE PAID_PUBLIC_HOLIDAYS > 10;

-- Mostrar todos los datos de la tabla de días libras para aquellos países que tengan más de 10 festivos nacionales y más de 15 días de vacaciones a año

SELECT *
FROM PAID_DAYS_OFF
WHERE PAID_PUBLIC_HOLIDAYS > 10 AND PAID_LEAVE_DAYS > 15;

-- Mostrar las columnas id, género e ídnice de desarrollo humano para España y Portugal.

SELECT COUNTRY_ID, GENDER, HUMAN_DEVELOPMENT_INDEX
FROM HUMAN_DEVELOPMENTS
WHERE COUNTRY_ID = 'ESP' OR COUNTRY_ID = 'PRT';

-- Mostrar los datos de población de España entre 1990 y el 2000

SELECT COUNTRY_ID, HUMAN_DEVELOPMENT_INDEX, YEAR
FROM HUMAN_DEVELOPMENTS
WHERE COUNTRY_ID = 'ESP' AND YEAR >= 1990 AND YEAR <= 2000;

-- Mostrar los datos de la tabla de índice de desarrollo humano para aquellos países en que el índice no sea nulo.

SELECT COUNTRY_ID, HUMAN_DEVELOPMENT_INDEX
FROM HUMAN_DEVELOPMENTS
WHERE HUMAN_DEVELOPMENT_INDEX IS NOT NULL;

-- Mostrar todos los países de la tabla del ídnice de desarrollo humano para aquellos IDs cuyo nombre empeiza por T

SELECT COUNTRY_ID, HUMAN_DEVELOPMENT_INDEX
FROM HUMAN_DEVELOPMENTS
WHERE COUNTRY_ID LIKE 'T%'; 

-- Mostrar todos los países de la tabla del índice de desarrollo humano para aquellos IDs cuyo nombre contiene V

SELECT COUNTRY_ID, HUMAN_DEVELOPMENT_INDEX
FROM HUMAN_DEVELOPMENTS
WHERE COUNTRY_ID LIKE '%V%'; 

-- 3. SQL Queries I_ Keywords y funciones básicas (GROUP BY)

-- Obtener la suma de la población mundial en 2020

SELECT YEAR, SUM(POPULATION)
FROM POPULATIONS
WHERE YEAR = 2020
GROUP BY YEAR;

-- Obtener la media de los años esperados de escolarización por países para el año 2020

SELECT COUNTRY_ID, AVG(EXPECTED_YEARS_OF_SCHOOLING) AS Media_Anos_escolarizacion_2020
FROM HUMAN_DEVELOPMENTS
WHERE YEAR = 2020
GROUP BY COUNTRY_ID;

-- Obtener la media del índice de desarrollo humano por género entre los años 2000 y 2020

SELECT GENDER, AVG(HUMAN_DEVELOPMENT_INDEX) AS MEDIA_IDH_2000_2020
FROM HUMAN_DEVELOPMENTS
WHERE YEAR >=2000 AND YEAR <=2020
GROUP BY GENDER;

-- Obtener para cada país el mayor índice de desarrollo humano que haya tenido en su historia

SELECT COUNTRY_ID, MAX(HUMAN_DEVELOPMENT_INDEX)
FROM HUMAN_DEVELOPMENTS
GROUP BY COUNTRY_ID;

-- Obtener el promedio del índice de desarrollo humano, la esperanza de vida al nacer y la renta per capita, para todos los países entre los años 1990 y 2000

SELECT COUNTRY_ID, AVG(HUMAN_DEVELOPMENT_INDEX), AVG(LIFE_EXPECTANCY_AT_BIRTH), AVG(GROSS_NATIONAL_INCOME_PER_CAPITA)
FROM HUMAN_DEVELOPMENTS
WHERE YEAR >= 1990 AND YEAR<= 2000
GROUP BY COUNTRY_ID;

-- 4. SQL Queries I: Jeywords y funciones básicas (HAVING)

-- Obtener aquellos países y el índice de desarrollo humano para los países cuyo valor mínimo del índice de desarrollo humano esté por encima de 0.7 entre los años 2000 y 2020

SELECT COUNTRY_ID, MIN(HUMAN_DEVELOPMENT_INDEX)
FROM HUMAN_DEVELOPMENTS
WHERE YEAR >= 2000 AND YEAR <= 2020
GROUP BY COUNTRY_ID
HAVING MIN(HUMAN_DEVELOPMENT_INDEX) > 0.7;

-- Obtener para el género femenino los países y el promedio de los años de escolarización para aquellos países cuyo promedio de los años de escolarización esperados para las mujeres sean mayor de 10 en los años 90.

SELECT COUNTRY_ID, AVG(EXPECTED_YEARS_OF_SCHOOLING) AS MEDIA_ANOS_ESCOLARIZACION_MUJERES, YEAR
FROM HUMAN_DEVELOPMENTS
WHERE GENDER = 'Female' AND YEAR >= 1990 AND YEAR < 2000
GROUP BY COUNTRY_ID, GENDER, YEAR
HAVING AVG(EXPECTED_YEARS_OF_SCHOOLING) >10;

-- Obtener los países y la renta per capita para aquellos países cuyo promedio de la renta per capita de los años 90 sea ayor a 35000

SELECT COUNTRY_ID, AVG(GROSS_NATIONAL_INCOME_PER_CAPITA)
FROM HUMAN_DEVELOPMENTS
WHERE YEAR >= 1990 AND YEAR < 2000
GROUP BY COUNTRY_ID
HAVING AVG(GROSS_NATIONAL_INCOME_PER_CAPITA) > 35000;

-- Obtener el promedio por país y género de la esperanza de vida al nacer para aquellos países que tengan un promedio del índice de desarrollo humano menor 0.7 en los años 90

SELECT COUNTRY_ID, GENDER, AVG(LIFE_EXPECTANCY_AT_BIRTH)
FROM HUMAN_DEVELOPMENTS
WHERE YEAR >= 1990 AND YEAR < 2000
GROUP BY COUNTRY_ID, GENDER
HAVING AVG(HUMAN_DEVELOPMENT_INDEX) < 0.7;

-- 5. SQL Queries I: Keywords y funciones básicas (ORDER BY)

-- Obtener una lista de países, el índice de desarrollo humano y la esperanza de vida al nacer ordenada de manera descendente en función del promedio del índice de desarrollo humano. Filtrar la información para el año 2020 y los valores del HDI nulos

SELECT COUNTRY_ID, AVG(HUMAN_DEVELOPMENT_INDEX), AVG(LIFE_EXPECTANCY_AT_BIRTH)
FROM HUMAN_DEVELOPMENTS
WHERE YEAR = 2020 AND HUMAN_DEVELOPMENT_INDEX IS NOT NULL
GROUP BY COUNTRY_ID
ORDER BY AVG(HUMAN_DEVELOPMENT_INDEX) DESC;

-- Obtener una lista con los países y el promedio de los años esperados de escolarización ordenada de manera descendente en base al promedio de los años esperados de escolarización. Filtrar la información para los años 90 y mostrar solo aquellos países cuyos años esperados de escolarización sean menos de 7.

SELECT COUNTRY_ID, AVG(EXPECTED_YEARS_OF_SCHOOLING)
FROM HUMAN_DEVELOPMENTS
WHERE YEAR >= 1990 AND YEAR < 2000
GROUP BY COUNTRY_ID
HAVING AVG(EXPECTED_YEARS_OF_SCHOOLING) < 7
ORDER BY AVG(EXPECTED_YEARS_OF_SCHOOLING) DESC;

-- Obtener una lista con los países y su promedio de la renta per capita y ordenar el resultado de manera descendente en base al promedio de la renta per capita. Filtrar para los años 2000 y diferencias el resultado entre hombres y mujeres.

SELECT COUNTRY_ID, GENDER,AVG(GROSS_NATIONAL_INCOME_PER_CAPITA)
FROM HUMAN_DEVELOPMENTS
WHERE YEAR >= 2000 AND YEAR <2010
GROUP BY COUNTRY_ID, GENDER
ORDER BY AVG(GROSS_NATIONAL_INCOME_PER_CAPITA) DESC;

-- Obtener una lista de países, promedio de esperanza de vida, y promedio de renta per capita ordenada de manera descendente en base a la esperanza de vida. Filtrar la información para los años 2000 y mostrar solo aquellos países cuyo promedio de la renta per capita sea mayor a 45000

SELECT COUNTRY_ID, AVG(LIFE_EXPECTANCY_AT_BIRTH), AVG(GROSS_NATIONAL_INCOME_PER_CAPITA)
FROM HUMAN_DEVELOPMENTS
WHERE YEAR >= 2000 AND YEAR < 2010 
GROUP BY COUNTRY_ID
HAVING AVG(GROSS_NATIONAL_INCOME_PER_CAPITA) > 45000
ORDER BY AVG(LIFE_EXPECTANCY_AT_BIRTH) DESC;

-- 6. Unir tablas JOINS

-- Realizamos una consulta donde se muestra el país, el género, el índice del desarrollo humano, la esperanza de vida y los dias libres al año. Filtramos para el año 2020 y mostramos solo los países en que el índice del desarrollo humano no sea nulo. Mostramos sólo los datos de la tabla del índice de desarrollo humano para aquellos países en que tengamos datos en las dos tablas.

SELECT H.COUNTRY_ID, GENDER, HUMAN_DEVELOPMENT_INDEX, LIFE_EXPECTANCY_AT_BIRTH, PAID_LEAVE_DAYS
FROM HUMAN_DEVELOPMENTS AS H JOIN PAID_DAYS_OFF AS P
ON H.COUNTRY_ID = P.COUNTRY_ID;

-- Realizamos una consulta donde se muestra el país, el promedio del índice del desarrollo humano, el promedio de la esperanza de vida y el promedio de la población. Mostramos los valores para los años 90 y solo para aquellos países que tengan un promedio del índice del desarrollo humano mayor de 0.75


SELECT H.COUNTRY_ID, AVG(HUMAN_DEVELOPMENT_INDEX), AVG(LIFE_EXPECTANCY_AT_BIRTH), AVG(POPULATION)
FROM HUMAN_DEVELOPMENTS AS H JOIN POPULATIONS AS P
ON H.COUNTRY_ID = P.COUNTRY_ID
GROUP BY H.COUNTRY_ID;

-- Realizamos una consulta donde mostramos el país, la región, el promedio de población, el promedio de días libres y el promedio del índice de desarrollo humano. Filtramos para los años más allá del 2000 y el género masculino. Mostramos todos los países de los que tengamos datos en la tabla población aunque no tengamos datos en la tabla de índice de desarrollo humano.
SHOW TABLES;

SELECT H.COUNTRY_ID, C.REGION, AVG(POPULATION), AVG(PAID_LEAVE_DAYS),AVG(HUMAN_DEVELOPMENT_INDEX), H.YEAR
FROM HUMAN_DEVELOPMENTS AS H 
JOIN COUNTRIES AS C ON H.COUNTRY_ID = C.COUNTRY_ID
JOIN POPULATIONS AS P ON H.COUNTRY_ID = P.COUNTRY_ID
JOIN PAID_DAYS_OFF AS PD ON H.COUNTRY_ID = PD.COUNTRY_ID
WHERE H.YEAR >= 2000 AND GENDER = 'Male'
GROUP BY H.COUNTRY_ID, C.REGION, H.YEAR;

-- Creamos una consulta en donde categorizamos los países en base a los siguientes criterios
-- + Si el índice de desarrollo humano es mayor que 0.8 y sus días libres son más de 20 "Top Country"
-- + Si el índice de desarrollo humano está entre 0.7 y 0.8 y sus días libres son más de 15 "Medium Country"
-- + Si el índice de desarrollo umano es menor que 0.7 "Bottom Country"
-- + El resto "Other"
-- Mostramos el país, el índide de desarrollo humano, el total de días libres y su categoría. Filtramos los datos para los años 90

SELECT H.COUNTRY_ID, HUMAN_DEVELOPMENT_INDEX, PAID_LEAVE_DAYS, H.YEAR,
    CASE 
        WHEN HUMAN_DEVELOPMENT_INDEX > 0.8 AND PAID_LEAVE_DAYS > 20 THEN 'Top Country'
        WHEN HUMAN_DEVELOPMENT_INDEX <=0.8 AND HUMAN_DEVELOPMENT_INDEX >=0.7 AND PAID_LEAVE_DAYS > 15 THEN 'Medium Country'
        WHEN HUMAN_DEVELOPMENT_INDEX < 0.7 THEN 'Bottom Country'
        ELSE 'Other'
    END AS CATEGORIA
FROM HUMAN_DEVELOPMENTS AS H JOIN PAID_DAYS_OFF AS P
ON H.COUNTRY_ID = P.COUNTRY_ID
WHERE YEAR >= 1990 AND YEAR < 2000;

-- 7. Unir tablas: Set Operators

-- Creamos una tabla donde unimos el resultado de dos consultas:
-- + Seleccionamos el id del país y el índice de desarrollo humano para el año 2020
-- + Seleccionamos el id del país y la población para el año 2020
-- Indicamos con un literal a que medida corresponde cada registro

SELECT COUNTRY_ID, HUMAN_DEVELOPMENT_INDEX
FROM HUMAN_DEVELOPMENTS
WHERE YEAR = 2020
UNION 
SELECT COUNTRY_ID, POPULATION
FROM POPULATIONS
WHERE YEAR = 2020;

-- Creamos una tabla en que solo mostremos aquellos países que se encuentran en la tabla de población y días libres. Aquellos que no esten en las dos no los mostramos

SELECT COUNTRY_ID
FROM PAID_DAYS_OFF
INTERSECT 
SELECT COUNTRY_ID
FROM POPULATIONS;

-- Creamos una tabla en que solo mostremos aquellos países que no se encuentren en las dos tablas de población y días libres. 

SELECT COUNTRY_ID
FROM POPULATIONS
EXCEPT 
SELECT COUNTRY_ID
FROM PAID_DAYS_OFF;

-- 8. SQL Queries II (CASE)

-- Creamos una categorización de los datos que identifica el año 2020 como "Current Year" y el resto como "Otros" y mostramos el promedio de índice de desarrollo humano para estos tres años.

SELECT COUNTRY_ID,YEAR, AVG(HUMAN_DEVELOPMENT_INDEX), 
    CASE 
        WHEN YEAR = 2020 THEN 'Current Year'
        ELSE 'Otros'
    END AS "AÑO ACTUAL" 
FROM HUMAN_DEVELOPMENTS
GROUP BY COUNTRY_ID, YEAR;

-- Creamos una categorización de los datos que identifica los países del G7 como "G7" y el resto de países como "Otros", hacemos un conteo del número de países y mostramos su promedio del índice de desarrollo humano y su renta per cápita. Filtramos la información para el año 2020 e incluimos los países con el índice de manera ascendente.

SELECT COUNT(COUNTRY_ID)/2 AS NUMERO_PAISES,AVG(HUMAN_DEVELOPMENT_INDEX),AVG(GROSS_NATIONAL_INCOME_PER_CAPITA),
    CASE 
        WHEN COUNTRY_ID = 'CAN' THEN 'G7'
        WHEN COUNTRY_ID = 'FRA' THEN 'G7'
        WHEN COUNTRY_ID = 'DEU' THEN 'G7'
        WHEN COUNTRY_ID = 'ITA' THEN 'G7'
        WHEN COUNTRY_ID = 'JPN' THEN 'G7'
        WHEN COUNTRY_ID = 'GBR' THEN 'G7'
        WHEN COUNTRY_ID = 'USA' THEN 'G7'
        ELSE 'Otros'
    END AS CATEGORIA
FROM HUMAN_DEVELOPMENTS
WHERE YEAR = 2020
GROUP BY CATEGORIA;

-- Creamos una categorización de los datos que identifica los países con una esperanza de vida media mayor que 80 como "Alta" entre 60-80 "Media" y por debajo de 60 "Baja". Calculamos el promedio del índice de desarrollo humano para estas tres categorías filtrando para el año 2020 y ordenándose en base al índice de manera ascendente.

SELECT AVG(HUMAN_DEVELOPMENT_INDEX), 
    CASE
        WHEN LIFE_EXPECTANCY_AT_BIRTH > 80 THEN 'ALTA'
        WHEN LIFE_EXPECTANCY_AT_BIRTH <= 80 AND LIFE_EXPECTANCY_AT_BIRTH >= 60 THEN 'MEDIA'
        ELSE ' BAJA'
    END AS NIVEL_ESPERANZA_VIDA
FROM HUMAN_DEVELOPMENTS
WHERE YEAR = 2020
GROUP BY NIVEL_ESPERANZA_VIDA
ORDER BY AVG(HUMAN_DEVELOPMENT_INDEX) ASC;

 
-- 9. SQL QUERIES II: Vistas (VIEWS)

-- Creamos una vista con la query que hemos desarrollado en el último ejercicio del apartado de Sub-Queries con el nombre de country_category. Hacemos una query a nuestra vista donde contemos el número de países que tenemos en cada categoria

CREATE OR REPLACE VIEW COUNTRY_CATEGORY AS
SELECT AVG(HUMAN_DEVELOPMENT_INDEX)AS INDICE_DESARROLLO_HUMANO, 
    CASE
        WHEN LIFE_EXPECTANCY_AT_BIRTH > 80 THEN 'ALTA'
        WHEN LIFE_EXPECTANCY_AT_BIRTH <= 80 AND LIFE_EXPECTANCY_AT_BIRTH >= 60 THEN 'MEDIA'
        ELSE ' BAJA'
    END AS NIVEL_ESPERANZA_VIDA
FROM HUMAN_DEVELOPMENTS
WHERE YEAR = 2020
GROUP BY NIVEL_ESPERANZA_VIDA
ORDER BY AVG(HUMAN_DEVELOPMENT_INDEX) ASC;