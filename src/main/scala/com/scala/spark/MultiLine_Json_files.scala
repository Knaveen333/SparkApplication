package com.scala.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MultiLine_Json_files {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("DataFrameOperation").master("local[*]").getOrCreate()

      val pathsToRead = List("E:\\HADOOP\\data-master\\retail_db\\sample1.json","E:\\HADOOP\\data-master\\retail_db\\sample2.json",
                             "E:\\HADOOP\\data-master\\sample3.json","E:\\HADOOP\\data-master\\sample4.json")

      val finalDF = pathsToRead.map(path => {
        spark.read.json(path)
      }).reduce(_ unionByName _)
        finalDF.show()
  }

}
