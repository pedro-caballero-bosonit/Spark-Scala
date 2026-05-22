package com.streamflix

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import com.streamflix.processor._ //importo los procesadores


object Main {
  def main(args: Array[String]): Unit = {
    //1. Verificamos que pasemos un argumento
    if (args.length == 0) {
      println(("Error: Indica el módulo. Ejemplo: sbt \"run 1\""))
      System.exit(1)
    }
    val modulo = args(0)

    //2. Creo la SparkSession IMPLÍCITA
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("StreamFlix")
      .master("local[*]")
      .getOrCreate()

    //3. Creo el SparkContext IMPLÍCITO, que necesitamos para los RDDs
    implicit val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    modulo match {
      case "1" => Module1Processor.run()
      case "2" => Module2Processor.run()
      case "3" => Module3Processor.run()
      case "4" => Module4Processor.run()
      case "5" => Module5Processor.run()
      case _ => println(s"El módulo $modulo no existe todavia.")
    }
    spark.stop()
  }
}