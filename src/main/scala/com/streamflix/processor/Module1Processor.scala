package com.streamflix.processor

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Module1Processor {
  private val VALID_PREFIXES = Set("[INFO]", "[ERROR]")

  def run()(implicit spark: SparkSession, sc: SparkContext): Unit = {
    println("=== MÓDULO 1: RDDs y Datos No Estructurados ===")

    // Tarea 0: Leer el archivo como un RDD de Strings
    val rawLogsRDD = sc.textFile("src/main/resources/data/server_logs.txt")

    // Tarea 1: Filtrar solo las líneas que empiezan con [ERROR] o [INFO]
    // Usamos .filter, que toma cada línea (a la que llamo 'linea') y evalúa una condición
    val validLogsRDD = rawLogsRDD.filter(linea =>
      VALID_PREFIXES.exists(linea.startsWith)
    )

    // Acción 1: para comprobar que funciona, uso take(5) para treaer las 5 primeras lineas
    println("--- Logs Válidos ---")
    validLogsRDD.take(5).foreach(println)

    // Tarea 2: Mapear para extraer (Nivel, Mensaje)
    val paresRDD = validLogsRDD.map(linea => {
      val partes = linea.split(" ", 2)
      val nivel = partes(0).replace("[", "").replace("]", "")
      val mensaje = partes(1)
      (nivel, mensaje)
    })

    // Acción 2: Muestro 3 lineas limpias
    println("--- Logs limpios ---")
    paresRDD.take(3).foreach(println)

    // Tarea 3: Contar cuántos errores de tipo 503 ocurrieron usando RDD actions (count, filter)
    val errores503RDD = paresRDD.filter(par => {
      val nivel = par._1
      val mensaje = par._2
      nivel == "ERROR" && mensaje.contains("503")
    })

    // Acción 3: Hago un .count para obtener un numero de erroes encontrados
    val total503 = errores503RDD.count()
    println(s"--- Errores 503 encontrados: $total503 ---")

    // Tarea 4: Calcular el porcentaje de errores respecto al total de logs válidos.
    val totalValidos  = validLogsRDD.count()
    val porcentaje = (total503.toDouble / totalValidos) * 100

    // Acción: Saco el total de logs validos y el porcentaje de error
    println(f"--- Logs validos totales: $totalValidos ---")
    println(f"--- Porcentaje de errores 503: $porcentaje%.2f%% ---")


    // Validación Manual:
      // Líneas descartadas (corruptas).
    val totalRaw = rawLogsRDD.count()
    val totalDescartadas = totalRaw - totalValidos

    println(s"Total líneas en archivo:  $totalRaw")
    println(s"Líneas válidas:           $totalValidos")
    println(s"Líneas descartadas:       $totalDescartadas")

    //1. solo ERROR
    val soloErroresRDD = paresRDD.filter(par => par._1 == "ERROR")
    //2. solo el codigo de error
    val codigosRDD = soloErroresRDD.map(par => {
      val mensaje = par._2
      val campos = mensaje.split("\\|")

      val codigo = campos
        .find(_.startsWith("Code:"))
        .map(_.replace("Code:", ""))
        .getOrElse("UNKNOWN")
      (codigo, 1L)
    })

    val conteoRDD = codigosRDD.reduceByKey(_ + _)

    //construyo la ruta absoluta dinamica
    val projectDir = System.getProperty("user.dir")
    val outputPath = s"file://$projectDir/src/main/resources/output/error_counts"

    //borro la carpeta si ya existe para evitar ERROR de HADOOP
    def borrarCarpeta(ruta: String): Unit = {
      val carpeta = new java.io.File(ruta)
      if(carpeta.exists()) {
        carpeta.listFiles().foreach(_.delete()) //borro los archivos de dentro
        carpeta.delete() //borro la carpeta vacia
      }
    }
    borrarCarpeta(s"$projectDir/src/main/resources/output/error_counts")

    //guardo la ruta absoluta
    conteoRDD
      .map(par => s"${par._1},${par._2}")
      .saveAsTextFile(outputPath)

    println(s"--- Archivo guardado en: $outputPath ---")
  }
}