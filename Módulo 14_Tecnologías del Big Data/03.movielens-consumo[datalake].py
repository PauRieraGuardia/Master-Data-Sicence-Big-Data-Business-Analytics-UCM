# Databricks notebook source
# MAGIC %md
# MAGIC # CONSUMO

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a simular el consumo de las recomendaciones 
# MAGIC
# MAGIC Para ello vamos a crear varias utilidades para visualizarlas

# COMMAND ----------

from IPython.display import HTML # Importa la clase HTML de YPython.display, necesaria para renderizar cadenas de texto como código HTML en la salida del notebook.
    
def display_recs(recs): # Define una función para obtener el HTML y mostrarlo
    html = ""
    for rec in recs:        
        html += f'''<p><iframe src="{rec['youtubeUrl']}"></iframe></p>'''
        html += f'''<p><a href="{rec['imdbUrl']}">{rec['title']}</a></p>'''
        html += f'''<p><a href="{rec['tmdbUrl']}">{rec['title']}</a></p>'''
    display(HTML(html))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Aumentamos la información de las recomendaciones haciendo cruces con varias tablas

# COMMAND ----------

def get_recs(user_id:int): # Creación de una nueva tabla utilizando las tablas creadas anteriormente
   recs = spark.sql(f"""
                 SELECT movie.*,
                        m.title,
                        m.year,
                        l.imdbUrl,
                        l.tmdbUrl,
                        t.youtubeUrl
                 FROM (SELECT explode(recommendations) as movie
                       FROM hive_metastore.recommender.user_recommendations
                       WHERE userId = {user_id}
                    ) r
                 LEFT JOIN hive_metastore.movielens.movies m ON r.movie.movieId=m.movieId   
                 LEFT JOIN hive_metastore.movielens.links l ON r.movie.movieId=l.movieId
                 LEFT JOIN hive_metastore.movielens.trailers t ON r.movie.movieId=t.movieId  
                 ORDER BY RATING DESC 
                 """)
   return recs.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC Mostramos las recomendaciones

# COMMAND ----------

display_recs(get_recs(user_id=0))