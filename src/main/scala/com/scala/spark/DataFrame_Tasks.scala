package com.scala.spark

import org.apache.spark.sql.SparkSession

object DataFrame_Tasks {

  def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder().appName("DataFrame Tasks").master("local[*]").getOrCreate()

      val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("C:\\Users\\Naveen\\IdeaProjects\\SparkDemo\\spark-warehouse\\emp_data.csv")
      df.show()
  }

}
