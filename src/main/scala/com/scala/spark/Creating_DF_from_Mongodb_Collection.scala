package com.scala.spark

import org.apache.spark.sql.SparkSession

object Creating_DF_from_Mongodb_Collection {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrame from Cassandra table")
       .master("local[*]")                                                       //https://www.youtube.com/watch?v=RhmgOul38iY
       .config("spark.mongodb.input.uri","mongodb://localhost:27017/navin_db.employee")  //Reading Mongodb collection as input
       .config("spark.mongodb.output.uri","mongodb://localhost:27017/navin_db.emp")      //Writing Mongodb collection as output
       .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

      val emp_df = spark.read.format("com.mongodb.spark.sql.DefaultSource")
        .option("database","navin_db")
        .option("collection","employee")
        .load()
       emp_df.printSchema()
       emp_df.show()

      emp_df.write.format("com.mongodb.spark.sql.DefaultSource")
          .mode("Overwrite")
          .option("database","navin_db")
          .option("collection","emp")
          .save()
    spark.stop()
  }

}
