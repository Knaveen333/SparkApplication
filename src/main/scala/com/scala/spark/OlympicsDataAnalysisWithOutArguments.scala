package com.scala.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OlympicsDataAnalysisWithOutArguments {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("OlympicsDataAnalysisWithArguments")
      .getOrCreate()

    println("RDD Operations: Country Wise TotalMedals")
    println("****************************************")

    spark.sparkContext.setLogLevel("WARN")

    val textFile = spark.sparkContext.textFile("E:\\HADOOP\\data-master\\Olympics\\olympics.csv")
    val counts = textFile.filter {
      x => {
        if (x.toString().split(",").length >= 10)
          true else false
      }
    }.map(line => {
      line.toString().split(",")
    })

    val fil = counts.filter(x => {
      if (x(5).equalsIgnoreCase("swimming") && (x(9).matches(("\\d+"))))
        true
      else
        false
    })
    val pairs: RDD[(String, Int)] = fil.map(x => (x(2), x(9).toInt))
    val pair_cnt = pairs.reduceByKey(_ + _).collect()
    pair_cnt.take(10).foreach(println)
  }
}
/*  RDD Operations: Country Wise TotalMedals
    ****************************************
         (Australia,163)
         (Hungary,9)
         (Argentina,1)
         (Brazil,8)
         (Croatia,1)
         (Canada,5)
         (Lithuania,1)
         (Japan,43)
         (Zimbabwe,7)
         (China,35) */
