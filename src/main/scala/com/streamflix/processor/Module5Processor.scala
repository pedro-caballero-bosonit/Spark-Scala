package com.streamflix.processor

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

object Module5Processor {
  def run()(implicit spark: SparkSession, sc: SparkContext): Unit = {
    println("=== MÓDULO 5: Parquet y Particionamiento ===")

    //1. Cargo logs igual que en módulos anteriores
    val rawLogsRDD = sc.textFile("src/main/resources/data/server_logs.txt")

    val logsRDD = rawLogsRDD
      .filter(_.startsWith("[INFO]"))
      .flatMap(linea => {
        val campos = linea.split(" ", 2)(1).split("\\|")
        if (campos.length == 4 && campos(2).startsWith("Play:Movie_")) {
          try {
            Some((
              campos(1).replace("User:", "").toLong,
              campos(2).replace("Play:Movie_", "").toLong,
              campos(3).replace("Dur:", "").toLong,
              campos(0)
            ))
          } catch { case _: Exception => None }
        } else None
      })

    import spark.implicits._
    val logsDF = logsRDD.toDF("user_id", "movie_id", "duration_watched", "timestamp")

    //2. Cargo movies igual que en módulos anteriores
    val customSchema = StructType(Array(
      StructField("id", LongType,   nullable = false),
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

    //3. Join igual que en Módulo 3
    val enrichedDF = logsDF.join(
      broadcast(moviesDF),
      logsDF("movie_id") === moviesDF("id"),
      "inner"
    )

    println(s"Registros tras el join: ${enrichedDF.count()}")
    enrichedDF.show(5)

    //4. creo la columna 'year' con la funcion year() para sacar el año del timestamp original
    val finalReportDF = enrichedDF
      .withColumn(
        "year",
        year(col("timestamp")))

    println("--- Guardo los datos particionados en formato parquet ---")

    //5. Guardado de los datos
    finalReportDF
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "country")
      .parquet("src/main/resources/output/analytics_warehouse")

    println("--- Escritura completada! ---")
  }
}
