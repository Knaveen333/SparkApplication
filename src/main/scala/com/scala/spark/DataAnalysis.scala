package com.scala.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, DoubleType}
import org.apache.spark.sql.functions._

object DataAnalysis {

  def main(args: Array[String]): Unit = {

       val spark = SparkSession.builder()
      .appName("Creating DataFrame_Operations_Union_Intersect")
      .master("local[*]")
      .getOrCreate()

       spark.sparkContext.setLogLevel("ERROR")

       val orders_seq = Seq(
             Row(1, "Visa", "Appliances",112.15, "2017-05-31 08:30:45", 3),
             Row(2, "Maestro", "Electronics",1516.18, "2017-07-28 11:30:20", 1),
             Row(3, "Visa", "Computers & Accessories",142.67, "2018-03-25 17:45:15", 6),
             Row(4, "MasterCard", "Electronics",817.15, "2018-06-12 06:30:35", 5),
             Row(5, "Maestro", "Garden & Outdoors",54.17, "2018-11-05 22:30:45", 1),
             Row(6, "Visa", "Electronics",112.15, "2019-01-26 20:30:45", 2),
             Row(7, "MasterCard", "Appliances",4562.37, "2019-02-18 08:56:45", 5),
             Row(8, "Visa", "Computers & Accessories",142.67, "2019-07-24 09:33:45", 8),
             Row(9, "MasterCard", "Books",200.05, "2019-09-22 08:40:55", 10),
             Row(10, "MasterCard", "Electronics",563.15, "2019-11-19 19:40:15", 3))

       val orders_schema = StructType(Array(
                           StructField("order_id", IntegerType, true),
                           StructField("order_type", StringType, true),
                           StructField("product_category", StringType, true),
                           StructField("order_amount", DoubleType, true),
                           StructField("order_datetime", StringType, true),
                           StructField("user_id", IntegerType, true)))

        val orders_df = spark.createDataFrame(spark.sparkContext.parallelize(orders_seq),orders_schema)
        orders_df.show(numRows = 5, truncate = false)
        orders_df.printSchema()

  }
}
