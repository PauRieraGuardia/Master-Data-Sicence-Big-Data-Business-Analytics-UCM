-- Vamos a realizar una introducción al entrono de Snowflake y la carga de nuestros datos iniciales

-- Por ello debemos definir los siguientes conceptos:
-- Contexto: Establece el entorno de ejecución o configuración de la sesión SQL.
-- Esquema: Organiza y agrupa objetos de base de datos como tablas, vistas, etc.
-- Warehouse: Un sistema de almacenamiento de datos para análisis y toma de decisiones, generalmente con grandes volúmenes de datos.

-- En primer lugar hay que seleccionar el contexto donde vamos a trabajar, para ello lo realizamos en la parte superior izquierda. Seleccionamos 
-- SNOWBEARAIR_DB.MODELED, esquema MODELED
-- Vamos a crear un virtual warehouse y configurarlo en el contexto. Antes de poder ejecutar un código SQL necesitamos un virtual warehouse.
-- En la parte superior derecha del entornio podemos ver si existe o no un warehouse creado

-- Con el siguiente codigo creamos el warehouse

CREATE WAREHOUSE GORILLA_wh WITH
    WAREHOUSE_SIZE = XSmall
    INITIALLY_SUSPENDED = true;

-- Con el siguiente cdigo empezaremos a utilizar el warehouse creado, veremos que en la parte superior derecha pasara de 
-- No warehouse selected a seleccionar el warehouse cread

USE WAREHOUSE GORILLA_wh;

-- Este contexto sirve basicamente para realiar querys
-- La primera que vamos a relizar es aquella que nos enseña todas las tablas disponibles en nuestro contexto actual, 
-- En este caso SNOWBEARAIR_DB.MODELED, esquema MODELED, que se encuentra en la parte superior izquierda

SHOW TABLES;

-- Vamos a realizar una serie de querys sobre los datos de SNOWBEARAIR.MODELED

SELECT * -- Seleccionamos todo el contexto SNOWBEARAIR_DB.MODELED en el esquema MODELED
FROM members -- Seleccionamos la tabla members
LIMIT 10; -- visualizamos los 10 primeros elementos de la tabla members


-- Vamos a crear distintos objetos.
-- Vamos a crear nuestro database. 
CREATE DATABASE GORILLA_DB;

-- Para usar la base de datos creada

USE DATABASE GORILLA_DB;

-- Vamos a crear un nuevo esquema para nuestro database GORILLA_wh, hasta ahora estabamos utilzando el esquema MODELED de SNOWBEARAIR_DB.MODELED
-- Al crear un nuevo database, el contexto pasa a formar parte del database creado (en nuestro caso GORILLA_DB.PUBLIC). Por ello necesitamos crear un nuevo esquema para el contexto (le llamaremos ONU).

-- Si hacemos un show tables, puesto que nos encintramos en el contexto GORILLA_DB,PUBLIC no nos devolvera ningún resultado.
SHOW TABLES;

CREATE SCHEMA ONU;

-- Ahora debemos usarl el shcema, o usamos la función siguiente o lo seleccionarmos en la parte superior izquierda.

USE SCHEMA ONU;

SHOW TABLES;
