package com.scala.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, DoubleType, StructType}
import org.apache.spark.sql.functions._

object Creating_dfs_operations {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Create_First_Spark_DataFrame")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val users_seq = Seq(Row(1,"Nani","Hyderabad"),
      Row(2,"Lakshmi","Chennai"),
      Row(3,"Sandy","Delhi"),
      Row(4,"Rani","Hyderabad"),
      Row(5,"Mani","Delhi"))

    val users_schema = StructType(Array(
      StructField("user_id", IntegerType, true),
      StructField("user_name", StringType, true),
      StructField("user_city", StringType, true)))

    val users_df = spark.createDataFrame(spark.sparkContext.parallelize(users_seq), users_schema)
    users_df.show(numRows = 5, truncate = false)

    val orders_seq = Seq(
      Row(1, "Visa", "Appliances",112.15, "2017-05-31 08:30:45", 3),
      Row(2, "MasterCard", "Electronics",1516.18, "2017-07-28 11:30:20", 1),
      Row(3, "Maestro", "Computers & Accessories",142.67, "2018-03-25 17:45:15", 6),
      Row(4, "Visa", "Electronics",817.15, "2018-06-28 12:30:35", 5),
      Row(5, "Maestro", "Garden & Outdoors",54.17, "2018-11-05 22:30:45", 1)
    )

    val orders_schema =   StructType(Array(
      StructField("order_id", IntegerType, true),
      StructField("card_type", StringType, true),
      StructField("order_category", StringType, true),
      StructField("order_amount", DoubleType, true),
      StructField("order_datetime", StringType, true),
      StructField("user_id", IntegerType, true)))

    val orders_df = spark.createDataFrame(spark.sparkContext.parallelize(orders_seq), orders_schema)
    orders_df.show(numRows = 5, truncate = false)
    orders_df.printSchema()

    println("Example 1:")
    users_df.withColumn(colName = "gender", lit(literal = "Male")).select(col = "*").show(numRows = 5, truncate = false)

    println("Example 2:")
    orders_df.withColumn(colName = "order_amount_with_tax", orders_df("order_amount") + 10).select(col = "*").show(numRows = 5, truncate = false)

    println("Example 3:")
    orders_df.withColumn(colName = "reference_no", lit(literal = 100)).select(col = "*").show(numRows = 5, truncate = false)

    spark.stop()
    println("Spark Application Completed...")

  }
}
