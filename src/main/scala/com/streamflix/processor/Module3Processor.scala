package com.streamflix.processor

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Module3Processor {
  def run()(implicit spark: SparkSession, sc: SparkContext): Unit = {
    //Tarea 1.
    println("=== MÓDULO 3: Joins & Broadcast ===")
    //archivo crudo
    val rawLogsRDD = sc.textFile("src/main/resources/data/server_logs.txt")
    //solo filtro por INFO
    val infoLogsRDD = rawLogsRDD.filter(_.startsWith("[INFO]"))

    println(s"Total lineas INFO: ${infoLogsRDD.count()}")
    infoLogsRDD.take(3).foreach(println)

    //Tarea 3.

    //Ahora tenemos esto: [INFO] 2025-11-13 18:00:00|User:1001|Play:Movie_1|Dur:40
    //Queremos esto: (1001, 1, 40, "2025-11-13 18:00:00")

    //usamos .flatMap para eliminar las lineas que no se pueden transformar
    val logsRDD = infoLogsRDD.flatMap(linea => {
      val mensaje = linea.split(" ", 2)(1)
      //utilizamos esto para descartar el [INFO] y quedarnos con la otr parte (1)

      val campos = mensaje.split("\\|")
      //separamos el codigo por |
        //campos(0) → "2025-11-13 18:00:00"
        //campos(1) → "User:1001"
        //campos(2) → "Play:Movie_1"
        //campos(3) → "Dur:40"

      if (campos.length == 4 && campos(2).startsWith("Play:Movie_")){
        try{
          Some((
            campos(1).replace("User:", "").toLong, //"User:1001" → "1001" → 1001L
            campos(2).replace("Play:Movie_", "").toLong, //"Play:Movie_1" → "1" → 1L
            campos(3).replace("Dur:", "").toLong, //"Dur:40" → "40" → 40L
            campos(0) // "2025-11-13 18:00:00" — ya es String
          ))
        } catch { case _: Exception => None }
      } else None
    })
    //si la línea tiene 4 campos y el 3 campo es una pelicula (Play:Movie_)
      //se hace un try catch para convertir los campor en numeros
        //"Dur:40"  → replace → "40" → toLong → 40L
      //si falla devuelve None
    println(s"--- Logs parseados correctamente: ${logsRDD.count()} ---")
    logsRDD.take(3).foreach(println)
    //Salida: (1001, 1, 40, "2025-11-13 18:00:00")

    //Tarea 4. convierto el RDD a dataframe con sus columnas nombradas
    import spark.implicits._
    val logsDF = logsRDD.toDF("user_id", "movie_id", "duration_watched", "timestamp")

    //verificación
    println("--- Schema de logsDF ---")
    logsDF.printSchema()

    println("--- 5 primeras filas ---")
    logsDF.show(5)

    //Tarea 5. cargo el esquema de peliculas
    val customSchema = StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("title", StringType, nullable = false),
      StructField("genres", StringType, nullable = true),
      StructField("subscription_price", StringType, nullable = true),
      StructField("release_date", StringType, nullable = true),
      StructField("country", StringType, nullable = true)
    ))

    val moviesDF = spark.read
      .option("header", "true")
      .schema(customSchema)
      .option("mode", "DROPMALFORMED")
      .csv("src/main/resources/data/movies_metadata.csv")

    println(s"--- Movies cargadas: ${moviesDF.count()} ---")

    //Join con Broadcast explicito
    val enrichedDF = logsDF.join(
      broadcast(moviesDF), //fuerzo el broadcast de la tabla más pequeña
      logsDF("movie_id") === moviesDF("id"), //uno mediante el id de la pelicula
      "inner" //solo filas que matchean en cada tabla
    )

    println(s"--- Registros tras el join: ${enrichedDF.count()} ---")
    enrichedDF.show(5)

    //Tarea 6. Transformo la columna de generos
    val genreDF = enrichedDF
      .withColumn(
        "genres", //Nombre de la columna nueva
        explode( //para que quede una fila por cada genero
          split(col("genres"), "\\|") //PRIMERO convierto el array
        )
      )
      .groupBy("genres")
      .agg(
        sum("duration_watched") //sumo las horas de CADA genero
          .alias("total_hours")
      )
      .orderBy(col("total_hours").desc) //ordeno de mayor a menor

    enrichedDF.explain()

    print("--- Top géneros por horas ---\n")
    genreDF.show(10)

  }
}
