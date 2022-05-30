package com.scala.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DFOperations_Olympics_data {

  def main(args: Array[String]): Unit = {

       Logger.getLogger("org").setLevel(Level.OFF)
       Logger.getLogger("akka").setLevel(Level.OFF)

       val spark = SparkSession.builder().appName("DataFrameOperation").master("local[*]").getOrCreate()

       val df = spark.read.format("csv").option("header","true").option("inferSchema","true")
         .load("E:\\HADOOP\\data-master\\Olympics\\olympics.csv")
        df.printSchema()
        df.show(10)

        df.groupBy("country").sum("TotalMedals").show(5)

  }

}
