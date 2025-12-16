# Databricks notebook source
# MAGIC %md
# MAGIC #TRANSFORMACIÓN
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Vamos a crear un modelo de recomendación de películas usando el algoritmo **ALS** usando la funcionalidad de **Spark MLLib**
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/ml-collaborative-filtering.html

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos DataFrames a partir de los datos de las tablas

# COMMAND ----------

database = "movielens" # Define variable python
movies = spark.sql("select * from hive_metastore.movielens.movies").cache() # Guarda en un dataset usando sql la tabla movies creada anteriormente 
print(f"There are {movies.count()} movies in the datasets") # Cuenta cuantas películas hay en el dataset

# COMMAND ----------

ratings = spark.sql("select * from hive_metastore.movielens.ratings").cache() # Crea un nuevo Dataframe utilizando sql
print(f"There are {ratings.count()} ratings in the datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC Configuramos una semilla para hacer reproducible nuestro entrenamiento

# COMMAND ----------

seed = 42 # Definimos una nueva variable semilla con 42

# COMMAND ----------

# MAGIC %md
# MAGIC Separamos el dataset en training y test

# COMMAND ----------

(training, test) = ratings.randomSplit([0.8, 0.2], seed=seed) # Separamos training y test e el DF de ratings
print(f"Training: {training.count()}") # Conteamos training
print(f"Test: {test.count()}") # Conteamos test


# COMMAND ----------

display(training.limit(5)) # Muestra las primeras 5 filas de training 
display(test.limit(5)) # Muestra las primeras 5 filas de tes

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos una instancia del algoritmo

# COMMAND ----------

from pyspark.ml.recommendation import ALS  # Importación del algoritmo ALS en la libreria ML de pyspark

als = ALS(userCol="userId", # Columna ID
          itemCol="movieId", # Identificadores del artículo
          ratingCol="rating", # Clasificación real
          maxIter=5, # Número máximo de iteraciónes
          seed=seed, # Establece semilla
          coldStartStrategy="drop", #Estrategia de arranque en frío
          regParam=0.1, #Se usa para evitar el overfitting
          nonnegative=True) # Aplicación del modelo

# COMMAND ----------

# MAGIC %md
# MAGIC Puesto que lo que estamos intentando predecir es la valoración de un usuario para cada película, es un problema de regresión y por lo tanto vamos a usar **RMSE** como nuestra métrica de evaluación.
# MAGIC
# MAGIC Además vamos a configurar un grid para hacer la búsqueda de hyperparametros. En este caso, es un grid muy pequeño, de tan solo dos parametros para evitar entrenar muchos modelos

# COMMAND ----------

from pyspark.ml.tuning import * # Importacion de utilidades para el ajuste de hiperparámetros 
from pyspark.ml.evaluation import RegressionEvaluator # Importa la clase para evaluar modelos de regresión 

rmse_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="rating", metricName="rmse") # Métrica de rendimiento 

grid = (ParamGridBuilder().addGrid(als.rank, [5, 10]).build()) # Construcción maya de hiperparámetros

cv = CrossValidator(numFolds=3, estimator=als, estimatorParamMaps=grid, evaluator=rmse_evaluator, seed=seed) # Configuración de la validación cruzada

cv_model = cv.fit(training) #Entrenamiento del modelo

# COMMAND ----------

# MAGIC %md
# MAGIC Obtenemos las métricas de nuestros modelos

# COMMAND ----------

cv_model.avgMetrics # Obtención de métricas

# COMMAND ----------

# MAGIC %md
# MAGIC el segundo modelo tiene un error menor

# COMMAND ----------

best_model = cv_model.bestModel # Extracción del mejro modelo 
print(f"El mejor modelo se ha entrenado con rank = {best_model.rank}") # Printea el mejor modelo 

# COMMAND ----------

# MAGIC %md
# MAGIC Las métricas anteriores han sido calculadas con un pequeños subconjunto de datos al hacer validación cruzada.
# MAGIC
# MAGIC Vamos a evaluar nuestro modelo contra el conjunto de **test**

# COMMAND ----------

predictions= best_model.transform(test) # Generación de predicciones
rmse = rmse_evaluator.evaluate(predictions) # Evaluaciín del modelo
print(f"ALS RMSE: {rmse:.3}") # Printea los tres valores de la RMSE

# COMMAND ----------

# MAGIC %md
# MAGIC Nuestro error final es de **0.878**

# COMMAND ----------

# MAGIC %md
# MAGIC Este tipo de modelos crear las recomendaciones como resultado, por lo tanto vamos a volver a entrenarlo, esta vez, con todas las valoraciones.
# MAGIC
# MAGIC Voy a añadir datos de un usuario ficticio con id 0, incluyendo algunas valoraciones de películas.
# MAGIC
# MAGIC Para ello creamos un DataFrame a partir de una lista python

# COMMAND ----------

from datetime import datetime # Importación libreria datetime
myUserId = 0 # Define un ID de usuario para el nuevo usuario
now = datetime.now() # deifne la variable now con datetime
myRatedMovies = [
     (myUserId, 1214, 5, now), # Alien
     (myUserId, 480,  5, now), # Jurassic Park
     (myUserId, 260, 5, now),  # Star Wars: Episode IV - A New Hope
     (myUserId, 541, 5, now),  # Blade Runner
     (myUserId, 2571, 5, now), # Matrix, The
     (myUserId, 296,  5, now), # Pulp Fiction
     (myUserId, 356,  5, now), # Forrest Gump     
     (myUserId, 593, 5, now),  # Silence of the Lambs, The
] # Datos de la nueva encuensta

myRatingsDF = spark.createDataFrame(myRatedMovies, ['userId', 'movieId', 'rating','timestamp']) # Crea un data frame con los datos de la nueva encuesta
display(myRatingsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Juntamos los dos DataFrames.

# COMMAND ----------

allratings = ratings.unionByName(myRatingsDF) # Junta los dos dataframes
als.setRank(10) # Ajusta el hiperparámetro Rank
best_model = als.fit(allratings) # Entrena el modelo con el nuevo dataframe 

# COMMAND ----------

# MAGIC %md
# MAGIC # SERVICIO LAKEHOUSE

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC En esta primera versión, vamos a servir las recomendaciones directamente desde nuestro lakehouse / datalake
# MAGIC
# MAGIC Configuramos la ruta para escribir los datos en la capa **gold**
# MAGIC

# COMMAND ----------

container = "datos" # Selecciona el container
account = spark.conf.get("adls.account.name") # Selecciona la cuenta
gold_path = f"abfss://{container}@{account}.dfs.core.windows.net/gold" # Crea una nueva ruta con una nueva cartpeta dentro del contenedor

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos un schema de base de datos dedicado para nuestras recomendaciones

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.recommender; /* crea una nueva tabla */

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos 10 recomendaciones (peliculas) para todos los usuarios

# COMMAND ----------

user_recs = best_model.recommendForAllUsers(10) # muestra las primeras 10 recomendaciones para todos los usuarios
display(user_recs)

# COMMAND ----------

# MAGIC %md
# MAGIC Cuando hicimos la limpieza, escribimos el dato en el path de silver y después definimos las tablas para su consumo SQL
# MAGIC
# MAGIC Es posible hacer las dos cosas de una sola vez 

# COMMAND ----------

(user_recs
.write
.format("delta")
.mode("overwrite")
.option("path",f"{gold_path}/recommender/user_recommendations")
.saveAsTable("hive_metastore.recommender.user_recommendations")) # Guardamos la tabla en gold

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos 5 recomendaciones (usuarios) para todas las películas

# COMMAND ----------

movie_recs = best_model.recommendForAllItems(5)
display(movie_recs)


# COMMAND ----------

(movie_recs
.write
.format("delta")
.mode("overwrite")
.option("path",f"{gold_path}/recommender/movie_recommendations")
.saveAsTable("hive_metastore.recommender.movie_recommendations"))