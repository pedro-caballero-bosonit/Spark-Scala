package com.streamflix.processor

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Module2Processor {
  def run()(implicit spark: SparkSession, sc: SparkContext): Unit = {
    print("=== MÓDULO 2: DataFrames y Schemas ===")

    //Tarea: limpie y genere una estructura para el .csv
    val customSchema = StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("title", StringType, nullable = false),
      StructField("genres", StringType, nullable = true),
      StructField("subscription_price", StringType, nullable = true),
      StructField("release_date", StringType, nullable = true),
      StructField("country", StringType, nullable = true)
    ))

    //Leo el csv con el esquema creado
    val moviesDF = spark.read
      .option("header", "true")//primera fila: nombre de columnas
      .schema(customSchema)//aqui le doy el formato del esquema que creamos
      .option("mode", "DROPMALFORMED")//las filas que no encajan las descarta
      .csv("src/main/resources/data/movies_metadata.csv")

    moviesDF.printSchema()
    moviesDF.show(5)

    // TODO1: Crear una UDF o usar expresiones select para limpiar el precio (quitar '$' y castear a Double)
    println("--- .csv limpio sin $ ---")
    val moviesDFLimpio = moviesDF
      .withColumn(
        "subscription_price", //remplazo la columna
        regexp_replace(col("subscription_price"), "\\$", "")
          .cast(DoubleType) //de string a double
      )
    moviesDFLimpio.printSchema()
    moviesDFLimpio.show(5)

    // TODO2: Hacer un análisis de nulos y duplicados en las columnas
    println("--- Análisis de nulos ---")
    moviesDFLimpio.columns.foreach(columna => {
    //.columns devuelve un Array[String] con los nombres de las columnas, para cada una contamos los nulos que tiene
      val nulos = moviesDFLimpio.filter(col(columna).isNull).count()
      println(s"$columna: $nulos nulos")
    })

    println("\n--- Análisis de duplicados ---")
    val totalFilas = moviesDFLimpio.count()
    val filasUnicas = moviesDFLimpio.dropDuplicates().count()
    val duplicados = totalFilas - filasUnicas
    println(s"Filas totales: $totalFilas")
    println(s"Filas únicas: $filasUnicas")
    println(s"Filas duplicadas: $duplicados")

    // TODO3: Manejar nulos en 'genres' reemplazando por "Unknown"
    println("\n--- Nulos en la columna genre ---")
    val moviesDFFinal = moviesDFLimpio
      .na.fill("-Unknown-", Seq("genres"))
      //.na, es una clase que accede a funciones diseñadas para tratae faltantes
      //.fill, es la accion que remplaza "Unknown" por los valores nulos ->
      //Seq("genres") le pasamos la columna a tratar por .fill

    //verifico que no queden nulos en "genres"
    println("\n--- Nulos en genres después de fill ---")
    val nulosGenres = moviesDFFinal.filter(col("genres").isNull).count()
    println(s"\n--- Nulos en genres: $nulosGenres ---")

    //Validacion manual:
    println("\n--- Validacion manual: DoubleType vs DecimalType ---")
    //DoubleType:
    val preciosDouble = Seq(14.99, 9.99, 4.99)
    val sumaDouble    = preciosDouble.sum

    println("=== DOUBLE ===")
    println(f"14.99 + 9.99 + 4.99 = $sumaDouble%.17f")
    println("\n--- resultado final limpio ---")

    //DecimalType:
    val preciosBigDecimal = Seq(
      BigDecimal("14.99"),
      BigDecimal("9.99"),
      BigDecimal("4.99")
    )
    val sumaBigDecimal = preciosBigDecimal.sum

    println("\n=== BIGDECIMAL (equivalente a DecimalType en Spark) ===")
    println(f"14.99 + 9.99 + 4.99 = $sumaBigDecimal")

    println("\n--- Esquema final y 5 filas limpias ---")
    moviesDFFinal.printSchema()
    moviesDFFinal.show(40)
  }
}
