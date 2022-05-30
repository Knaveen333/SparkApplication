package com.scala.spark

import org.apache.spark.sql.SparkSession

object Creating_DF_From_Mysql_table {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Creating DF from Mysql table").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val mysql_db_driver_class = "com.mysql.jdbc.Driver"
    val table_name = "employee"
    val host_name = "localhost"
    val port_no = "3306"
    val user_name = "root"
    val password = "root"
    val database_name = "naveen"

    val mysql_select_query = "(select * from " + table_name + ") as employee"
    println("Printing mysql_select_query: ")
    println(mysql_select_query)

    val mysql_jdbc_url = "jdbc:mysql://" + host_name + ":" + port_no + "/" + database_name
    print("Printing JDBC Url: " + mysql_jdbc_url)

   val emp_df = spark.read.format("jdbc")
       .option("url",mysql_jdbc_url)
       .option("driver",mysql_db_driver_class)
       .option("dbtable",mysql_select_query)
       .option("user",user_name)
       .option("password",password)
       .load()
    emp_df.show()
    spark.stop()
  }

}
