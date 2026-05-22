package com.streamflix.processor

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Module4Processor {
  def run()(implicit spark: SparkSession, sc: SparkContext): Unit ={
    println("=== MÓDULO 4: Análisis de Comportamiento (Window Functions) ===")

    //Tarea 1. Cargo los logs igual que en el módulo 3
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
              campos(0)  // timestamp como String por ahora
            ))
          } catch { case _: Exception => None }
        } else None
      })

    import spark.implicits._
    val logsDF = logsRDD.toDF("user_id", "movie_id", "duration_watched", "timestamp")

    println(s"Logs cargados: ${logsDF.count()}")
    logsDF.show(5)

    //Tarea 2. Necesito saber cuanto tiempo paso entre una película y la anterior
    //defino una ventana que agrupa por usuario y ordena por tiempo
    val windowSpec = Window
      .partitionBy("user_id") //creo una ventana para cada usuario
      .orderBy("timestamp") // dentro de cada usuario, ordeno cronológicamente

    //añado una nueva columna con el timestamp de la reproduccion anterior
    val lagDF = logsDF.withColumn(
      "prev_timestamp", //nueva columna
      lag("timestamp", 1)
        .over(windowSpec) //dentro de la nueva columna
    )

    println("--- Logs con timestamp anterior ---")
    lagDF.show(10)

    //Tarea 3. Necesito saber si pasaron menos de 20 mins entre una reproduccion y la anterior, si es asi es Binge Watching
    // Calculo el tiempo de FIN de cada película en segundos
    val withEndTimeDF = lagDF.withColumn(
      "end_time_seconds",
      unix_timestamp(col("timestamp")) +  // inicio en segundos
        col("duration_watched") * 60        // duración convertida a segundos
    )

    // Traigo el end_time de la película ANTERIOR con lag
    val withPrevEndDF = withEndTimeDF.withColumn(
      "prev_end_time",
      lag("end_time_seconds", 1).over(windowSpec) // fin de la peli anterior
    )

    // Calculo la pausa REAL en minutos
    val diffDF = withPrevEndDF.withColumn(
      "diff_minutes",
      (unix_timestamp(col("timestamp")) - // inicio película actual
        col("prev_end_time"))              // fin película anterior
        / 60                                // convertimos a minutos
    )

    // is_binge = pausa menor a 20 minutos y positiva
    val bingeDF = diffDF.withColumn(
      "is_binge",
      when(
        col("diff_minutes") < 20 &&
          col("diff_minutes") > 0,  // > 0 descarta tiempos negativos (User:999)
        true
      ).otherwise(false)
    )

    println("--- Verificando sesiones de un usuario especifico ---")
    bingeDF
      .filter(
        col("user_id").isin(999)
      )
      .select("user_id", "timestamp", "duration_watched", "diff_minutes", "is_binge")
      .orderBy("user_id", "timestamp")
      .show(30)

    //Tarea 4. usuarios mas adictos
    val topBingeDF = bingeDF
      .filter(col("is_binge") === true) //solo filas donde hay binge
      .groupBy("user_id")
      .agg(count("*").alias("binge_count")) //contamos las líneas binge de cada usuario
      .orderBy(col("binge_count").desc)

    println("--- Top 10 Binge Watches ---")
    topBingeDF.show(10)

    println("--- Top 3 usuarios más adictos ---")
    topBingeDF.show(3)

    //Validacion manual
    println("--- Validación Manual ---")
    //User:999 que tiene tiempos negativos
    bingeDF
      .filter(col("user_id") === 999 && col("diff_minutes") < 0)
      .select("user_id", "timestamp", "diff_minutes", "is_binge")
      .show(5)
  }
}
