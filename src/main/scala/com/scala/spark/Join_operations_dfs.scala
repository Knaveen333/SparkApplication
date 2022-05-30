package com.scala.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, DoubleType, StructType}
import org.apache.spark.sql.functions._

object Join_operations_dfs {

  def main(args: Array[String]): Unit = {
    println("Spark Application Started...")

    val spark = SparkSession.builder()
      .appName("Creating DataFrame_Operations_join_outer_left_right")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("Example: 1")
    val users_seq = Seq(Row(1,"Nani","Hyderabad"),
      Row(2,"Lakshmi","Chennai"),
      Row(3,"Sandy","Delhi"),
      Row(4,"Rani","Hyderabad"),
      Row(5,"Mani","Delhi"),
      Row(6,"Sam","Chennai"),
      Row(7,"Sai","Delhi"))

    val users_schema = StructType(Array(
      StructField("user_id", IntegerType, true),
      StructField("user_name", StringType, true),
      StructField("user_city", StringType, true)))

    val users_df = spark.createDataFrame(spark.sparkContext.parallelize(users_seq), users_schema)
    users_df.printSchema()
    users_df.show(numRows = 10, truncate = false)

    println("Example: 2")
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
      StructField("card_type", StringType, true),
      StructField("product_category", StringType, true),
      StructField("order_amount", DoubleType, true),
      StructField("order_datetime", StringType, true),
      StructField("user_id", IntegerType, true)))

    val orders_df = spark.createDataFrame(spark.sparkContext.parallelize(orders_seq),orders_schema)
    orders_df.show(numRows = 5, truncate = false)
    orders_df.printSchema()

    spark.conf.set("spark.sql.crossJoin.enabled", true)

    println("Example: 3")
    val left_join_df_1 = orders_df.join(users_df, Seq("user_id"), joinType = "left").select( col = "user_id", cols = "order_id","card_type","product_category","order_amount","order_datetime","user_name","user_city")
    left_join_df_1.show(numRows = 10, truncate = false)

    println("Example: 4")
    import spark.implicits._
    val left_join_df_2 = orders_df.as( alias = "o").join(users_df.as( alias = "u"), joinExprs = ($"o.user_id" === $"u.user_id"), joinType = "left")
    left_join_df_2.show(numRows = 10, truncate = false)

    println("Example: 5")
    val right_join_df_1 = orders_df.join(users_df, Seq("user_id"), joinType = "right").select( col = "user_id", cols = "order_id","card_type","product_category","order_amount","order_datetime","user_name","user_city")
    right_join_df_1.show(numRows = 10, truncate = false)

    println("Example: 6")
    import spark.implicits._
    val right_join_df_2 = orders_df.as( alias = "o").join(users_df.as( alias = "u"), joinExprs = ($"o.user_id" === $"u.user_id"), joinType = "right")
    right_join_df_2.show(numRows = 10, truncate = false)

    println("Example: 7")
    val full_outer_or_outer_df_1 = orders_df.as( alias = "u1").join(users_df.as( alias = "u2"), joinExprs = ($"u1.user_id" === $"u2.user_id"), joinType = "outer")
    full_outer_or_outer_df_1.show(numRows = 10, truncate = false)

    println("Example: 8")
    val inner_join_df_1 = orders_df.join(users_df, Seq("user_id"), joinType = "inner").select( col = "user_id", cols = "order_id", "card_type", "product_category", "order_amount", "order_datetime", "user_name", "user_city")
    inner_join_df_1.show(numRows = 10, truncate = false)

    println("Example: 9")
    val inner_join_df_2 = orders_df.as( alias = "o").join(users_df.as( alias = "u"), joinExprs = ($"o.user_id" === $"u.user_id"), joinType = "inner")
    inner_join_df_2.show(numRows = 10, truncate = false)

    println("Example: 10")
    val  cross_join_df_1 = users_df.crossJoin(users_df)
    cross_join_df_1.show(numRows = 10, truncate = false)

    spark.stop()
    println("Spark Application Completed...")

//    println("Example: 11") //It will perform inner join [not cross join]
//    cross_join_df_2 = users_df.as( alias = "u1").join(users_df.as("u2"), joinExprs = ($"u1.user_id" === $"u2.user_id"), joinType = "cross")
//    cross_join_df_2.show(numRows = 10, truncate = false)

  }
}


