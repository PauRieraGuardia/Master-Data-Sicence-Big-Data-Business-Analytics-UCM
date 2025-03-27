-- 1. Configuramos el contexto, este proceso se puede hacer seleccionando o mediante código

USE ROLE TRAINING_ROLE; -- Asignamos el role que vamos a usar como usuario
USE WAREHOUSE GORILLA_WH; -- Asignamos el warehouse con el que vamos a trabajar
USE DATABASE GORILLA_DB; -- Asignamos el databaste con el que vamos a trabajar
USE SCHEMA ONU; -- Asignamos el schema con el que vamos a trabajar

-- 2. Creamos la tabla Human Development, tambien podemos crear una tabla directamente cargando un csv desde el apartado de data al inicio de snowflake

CREATE OR REPLACE TABLE HUMAN_DEVELOPMENTS ( -- Creamos o replazamos (en casoi de que exista) la tabla definiendola con su nombre
COUNTRY_ID VARCHAR(20), -- Cuando utilizamos varchar estamos definiendo la columna como columna de texto y () el número de caracteres maximo de ese texto
GENDER VARCHAR(20), -- Creamos columna de texto para gender
YEAR NUMBER(4,0), -- Creamos una columna de número con un número de caracteres entre 0 y 4
HUMAN_DEVELOPMENT_INDEX NUMBER(38,2),-- Creamos una columna de número llamada Human Development Index
LIFE_EXPECTANCY_AT_BIRTH NUMBER(38,2),
EXPECTED_YEARS_OF_SCHOOLING NUMBER(38,2),
MEAN_YEARS_OF_SCHOOLING NUMBER (38,2),
GROSS_NATIONAL_INCOME_PER_CAPITA NUMBER(38,2))
;

-- Realizamos una serie de querys

SHOW TABLES;

SELECT * 
FROM HUMAN_DEVELOPMENTS; 


-- Nos aparece una tabla vacia llamada human_developments en nuestro contexto gorilla_db esquema ONU ara Show tables, y nos selecciona la tabla vacia para SELECT * FROM HUMAN_DEVELOPMENTS 

-- 3. Estudiamos los tipos de datos

DESCRIBE TABLE HUMAN_DEVELOPMENTS;

-- Nos aparece en output los tipos de variables que tenemos por columna, como hemos definido anteriormente 

-- * Hemos realizado la carga de los datos mediante la carga del archivo csv a través del portal de data del inicio * 

-- 4. Cambiar el nombre de la columna GROS NATIONAL INCOME PER CAPITA A RPC

ALTER TABLE HUMAN_DEVELOPMENTS -- Seleccionamos la tabla que queremos modificar
RENAME COLUMN GROSS_NATIONAL_INCOME_PER_CAPITA TO RPC; -- renobramos la columna de la tabla que queremos modificar

-- 5. Cambiar el tipo de Year

ALTER TABLE
    human_developments RENAME COLUMN year TO year_temp; -- Renombramos la columna year a year_temp

ALTER TABLE
    human_developments 
ADD COLUMN year VARCHAR; -- Creamos una nueva columna year que sera un VARCHAR

UPDATE 
    human_developments
SET
    year = TO_CHAR(year_temp); -- Estamos pasando los datos de year_temp (renombrada anteriormente) como char a la columna year

ALTER TABLE 
    human_developments DROP COLUMN year_temp; -- Eliminamos la columna year_temp que contiene los años en NUMBER

DESCRIBE TABLE human_developments;

-- 6. Borrar la tabla Human Developments 

DROP TABLE human_developments; -- Eliminamos la tabla Human_developments, solo para la sesión.

SELECT * human_developments;

UNDROP TABLE human_developments; -- Recuperamos la tabla anteriormente eliminada

SHOW TABLES;

-- 7. Modificar algunas filas, eliminamos las filas con HDI nulos

SELECT * -- Seleccionamos toda la data de la tabla
FROM human_developments -- Seleccionamos la tabla human_developments
WHERE human_development_index IS NULL; -- Seleccionamos aquellos que tienen valor null en la columna Human_development_index

DELETE FROM HUMAN_DEVELOPMENTS -- Eliminas de la tabla human_developments
WHERE human_development_index IS NULL; -- Aquellos valores que son nulos en la columna human_development_index

SELECT *
FROM human_developments
WHERE human_development_index IS NULL; -- Vemos que no hay valores nulos en human_development_index

-- 8. Vamos a cambiar el vaor de Gender a Hombre y Mujer

UPDATE human_developments -- Actualizamos en la tabla human_developments
SET GENDER = 'Hombre'
WHERE Gender = 'Male'; -- Cambiamos a hombre donde la variable gender sea male

UPDATE human_developments
SET GENDER = 'Mujer'
WHERE Gender = 'Female';

SELECT Gender
FROM human_developments;

-- 9. Como vaciar una tabla

TRUNCATE TABLE HUMAN_DEVELOPMENTS; -- Vaciamos los datos de la tabla, pero sin eliminar la tabla

-- 10. Cambiar Nombre Tabla, o desde databases o aplicando el código directamente

ALTER TABLE HUMAN_DEVELOPMENTS RENAME TO HD;

SHOW TABLES;