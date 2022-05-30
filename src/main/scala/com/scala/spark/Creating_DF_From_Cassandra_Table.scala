package com.scala.spark

import org.apache.spark.sql.SparkSession

object Creating_DF_From_Cassandra_Table {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrame from Cassandra table").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val cassandra_host_name = "localhost"
    val cassandra_port_no = "9042"
    val cassandra_ks_name = "navin_ks"
    val cassandra_table = "employee"
    val cassandra_cluster_name = "Test Cluster"

    val emp_df = spark.read.format("org.apache.spark.sql.cassandra")  //Reading DF from Cassandra table
      .options(Map("table" -> cassandra_table,
                   "keyspace" -> cassandra_ks_name,
                   "host" -> cassandra_host_name,
                   "port" -> cassandra_port_no,
                   "cluster" -> cassandra_cluster_name))
      .load()
       emp_df.show()
       emp_df.write.format("org.apache.spark.sql.cassandra")        //Writing DF to Cassandra table first create the table Strecture
      .option("keyspace","navin_ks")
      .option("table","emp")
      .save()
  }

}
