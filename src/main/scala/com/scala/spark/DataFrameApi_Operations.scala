package com.scala.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col

object DataFrameApi_Operations {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

       val spark = SparkSession.builder().appName("Task1").master(("local[*]")).getOrCreate()

       val df = spark.read.option("header","true").option("sep","|").option("inferSchema","true").format("csv").load("C:/Users/Naveen/IdeaProjects/SparkDemo/spark-warehouse/user_data.csv")
    // val df = spark.read.option("header","true").option("sep","|").option("inferSchema","true").format("csv").load("C:\\Users\\Naveen\\IdeaProjects\\SparkDemo\\spark-warehouse\\user_data.csv")
        df.printSchema()
        df.show()

      df.filter(col("designation") === "technician").show()
      df.filter(col("designation") === "technician" && col("salary").cast("Int")>35000).show()
      df.groupBy(col("designation")).agg(avg("salary")).show()
      df.groupBy(col("designation")).agg(avg("salary").as ("avg")).show()
      df.groupBy(col("designation")).agg(avg("salary").as ("avg")).sort(col("avg").desc).limit(5).show()
      df.coalesce(1).write.format("csv").save("E:\\HADOOP\\data-master\\user.csv")
      df.sort(col("id").desc).show() //reverse order
      val df1 = df.withColumn("gender_expansion",when(col("gender")==="M","Male").otherwise("Female"))
      df1.drop("gender").show()
      df1.select("id","age","gender_expansion","designation","salary").show()

  }
}
