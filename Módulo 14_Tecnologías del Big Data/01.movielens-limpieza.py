# Databricks notebook source
# MAGIC %md
# MAGIC # LIMPIEZA

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE

# COMMAND ----------

# MAGIC %md
# MAGIC Accedemos al nombre de nuestra cuenta de almacenamiento a través de la variable de entorno que configuramos en nuestro cluster y creamos una variable apuntando al path de nuestro contenedor **datos** y a la carpeta **bronze**

# COMMAND ----------

container = "datos" # Seleccionamos el container de donde queremos sacar los adtos
account = spark.conf.get("adls.account.name") # L acuenta de ADLS
bronze_path = f"abfss://{container}@{account}.dfs.core.windows.net/bronze" # Definimos la ruta

# COMMAND ----------

print(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Comprobamos que tenemos acceso a la cuenta de almacenamiento

# COMMAND ----------

display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a crear Spark DataFrames a partir de los directorios de cada uno de los datatsets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Movies

# COMMAND ----------

 
movies_path = f"{bronze_path}/movielens/movies/" # Definimos la ruta donde se encuentra el dataset
movies_raw = (spark.read # Función para leer el archivo
              .option("header","true") # Indica que la primera fila del archivo contiene el header
              .option("escape","\"") # Indica que el carácter "" se usa para escapar caracteres especiales
              .option("inferSchema","true") # Pide a Spark que intente determinar el tipo de dato
              .csv(movies_path) # Lectura en formato csv
            )
display(movies_raw.limit(10)) # Muestra las primears 10 columnas

# COMMAND ----------

movies_raw.printSchema() # Imprime el esquema del DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ratings

# COMMAND ----------

ratings_path = f"{bronze_path}/movielens/ratings/" # Guarda la ruta del dataframe ratings
ratings_raw = (spark.read # Función para leer el dataframe
            .option("header","true") # Header true
            .option("inferSchema","true") # Le dice a Spark que intente inferir el tipo de dato
            .csv(ratings_path) # Lectrua del DataFrame formato csv
)

display(ratings_raw.limit(10)) # Muestra las 10 primeras columnas

# COMMAND ----------

ratings_raw.printSchema() # Printea el esquema

# COMMAND ----------

# MAGIC %md
# MAGIC ###Links

# COMMAND ----------

links_path = f"{bronze_path}/movielens/links/" # Dataframe links
links_raw = spark.read.option("header","true").csv(links_path)

display(links_raw.limit(10))

# COMMAND ----------

links_raw.printSchema() # Mostramos esquema

# COMMAND ----------

# MAGIC %md
# MAGIC ###Trailers

# COMMAND ----------

trailers_path = f"{bronze_path}/movielens/trailers/" # Dataframe trailers
trailers_raw = spark.read.option("header","true").option("inferSchema","true").csv(trailers_path)
display(trailers_raw.limit(10))

# COMMAND ----------

trailers_raw.printSchema() # Mostramos esquema

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora vamos a ir limpiando dataset a dataset aplicando pequeñas transformaciones de limpieza sobre cada Spark DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ### Movies

# COMMAND ----------

import re # Libreria de expresiones regulares
from pyspark.sql.functions import col,split # Importa funciones de Spark SQL: columnda y dividir

# Define una función definida por el usuario (UDF), que separa año y titulo de la columna 'title'
@udf("array<string>")
def parse_title(t:str):
    titleRegex = re.compile(r'^(.+)\((\d{4})\)$') # Expresión regular para capturar el Título y el Año al final de la cadena
    m = titleRegex.search(t.strip())
    if m:
        title,year= m.groups()
        return [title.strip(),year.strip()]
    else:
        return [t,None]
    
#Inicia la creación de un nuevo dataset pasandole la función definida para movies
movies_std = movies_raw.select(
    col("movieId").cast("bigint"), # Selecciona la columa "movieID" y la convierte al tipo de BigInt oara estandarizar
    parse_title(col("title"))[0].alias("title"), # Aplica la función, toma el primer elemento y le da el alias 'title'
    parse_title(col("title"))[1].cast("integer").alias("year"), # Aplica la función, toma el segundo elemento y lo convierte a entero y le da el alias 'year'.
    split("genres",'\|').alias("genres")
    )

display(movies_std) # Muestra todas las columnas de la tabla

# COMMAND ----------

display(movies_std.where(col("year").isNull())) # Muestra donde es nulo 

# COMMAND ----------

from pyspark.sql.functions import array_remove # Importación modulo array_remove

movies_std = movies_std.withColumn("genres",array_remove(col("genres"),"(no genres listed)")) # Limpia las peliculas donde 'genres' está listado como asuente

# COMMAND ----------

display(movies_std.where(col("year").isNull())) # Muestra donde es nulo 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ratings

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, from_unixtime # Importa funciones de Spark SQL para la manipulación del tiempo

# Crea un nuevo dataframe para la tabla ratings
ratings_std = ratings_raw.select(
    col("userId").cast("bigint"), # Selecciona la columna 'userID' y la convierte eal tipo BigInt
    col("movieId").cast("bigint"),  # Selecciona la columna 'userID' y la convierte eal tipo BigInt
    col("rating").cast("double"),     # Selecciona la columna 'userID' y la convierte eal tipo double
    to_timestamp(from_unixtime("timestamp")).alias("timestamp") # Selecciona la columna 'timestamp' y la convierte a timestamp
)
display(ratings_std) # muetra toda la tabla nueva de ratings

# COMMAND ----------

# MAGIC %md
# MAGIC ### Links

# COMMAND ----------

from pyspark.sql.functions import concat,lit # Importa concat y lit de la libreria Spark SQL

# Crea nuevo dataframe para lonks
links_std = links_raw.select(
    col("movieId").cast("bigint"),
    col("imdbId"),    
    concat(lit("http://www.imdb.com/title/tt"),col("imdbId"),lit("/")).alias("imdbUrl"), # Concatena dos columnas
    col("tmdbId"),    
    concat(lit("https://www.themoviedb.org/movie/"),col("tmdbId"),lit("/")).alias("tmdbUrl") # Concatena dos columnas
)

display(links_std)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trailers

# COMMAND ----------

from pyspark.sql.functions import * # importa * de la libreria Spark SQL

# Lo mismo para trailes 
trailers_std = trailers_raw.select(
    col("movieId").cast("bigint"),
    col("youtubeId"),    
    concat(lit("https://www.youtube.com/embed/"),col("youtubeId"),lit("/")).alias("youtubeUrl") # Concatena 
)
display(trailers_std)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Salvamos todas los DataFrames tranformados. 
# MAGIC
# MAGIC En este caso vamos a salvarlos en la capa **silver**
# MAGIC
# MAGIC Además vamos a salvar los datos en formato **delta** que es un formato optimizado para big data y analítica
# MAGIC

# COMMAND ----------

# Guardamos los dataframes en una nueva capa silver

silver_path = f"abfss://{container}@{account}.dfs.core.windows.net/silver/" # Creamos nuevo container para guardar

movies_std.write.format("delta").mode("overwrite").save(f"{silver_path}/movielens/movies/") # Crea una nueva carpeta movies

ratings_std.write.format("delta").mode("overwrite").save(f"{silver_path}/movielens/ratings/") # Crea una nueva carpeta ratings

links_std.write.format("delta").mode("overwrite").save(f"{silver_path}/movielens/links/") # Crea una nueva carpeta links

trailers_std.write.format("delta").mode("overwrite").save(f"{silver_path}/movielens/trailers/") # Crea una nueva carpeta trailers

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Los usuarios no suelen trabajar con ficheros ya que les implicaría recordar todas las rutas a cada uno de los ficheros.
# MAGIC
# MAGIC Publicamos los distintos datasets como tablas para facilitar su uso mediante SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Código SQL*/
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.movielens; /* Crea la base de datos hive_metasorte.movielens */
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.movielens.movies /* Crea una tabla movies en la database creada usando la ruta de movies */
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://datos@${adls.account.name}.dfs.core.windows.net/silver/movielens/movies';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.movielens.ratings /*Crea una tabla ratings en la database creada usando la ruta de ratings */
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://datos@${adls.account.name}.dfs.core.windows.net/silver/movielens/ratings';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.movielens.links /* Crea una tabla links en la database creada usando la ruta de links */
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://datos@${adls.account.name}.dfs.core.windows.net/silver/movielens/links';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.movielens.trailers /* Crea una tabla trailers en la database creada usando la ruta de trailers */
# MAGIC LOCATION 'abfss://datos@${adls.account.name}.dfs.core.windows.net/silver/movielens/trailers';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Código SQL */
# MAGIC
# MAGIC select *
# MAGIC from hive_metastore.movielens.movies where year = 1995; /* Selecciona toda de la tabla movies en el database creado donde el año sea 1995 */