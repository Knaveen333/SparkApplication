package com.scala.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Date_Functions_in_spark {

  def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder().appName("Date_Functions").master("local[*]").getOrCreate()

      val schema = new StructType().add("emp_id",IntegerType,true)
        .add("e_name",StringType,true)
        .add("DOJ",StringType,true)

      val df = spark.read.option("header",true).schema(schema).csv("C:\\Users\\Naveen\\IdeaProjects\\SparkDemo\\spark-warehouse\\emp_data.csv")
          df.show()

      val df1 = df.withColumn("DOJ",from_unixtime(df("DOJ")))

      val df2 = df1.withColumn("year",year(df1("DOJ")))
        .withColumn("month",month(df1("DOJ")))
        .withColumn("DayOfMonth",dayofmonth(df1("DOJ")))
        .withColumn("DayOfWeek",dayofweek(df1("DOJ")))
        .withColumn("Quarter",quarter(df1("DOJ")))
        .withColumn("LastDayOfTheMonth",last_day(df1("DOJ")))
        .withColumn("AddMonths",add_months(df1("DOJ"),2))
        .withColumn("AddedDate",date_add(df1("DOJ"),3))
        .withColumn("ReduceDays",date_sub(df1("DOJ"),4))
        .withColumn("Hour",hour(df1("DOJ")))
        .withColumn("Minute",minute(df1("DOJ")))
        .withColumn("Seconds",second(df1("DOJ")))
        .withColumn("CurrentTime",current_timestamp())
        .withColumn("DateFormat",date_format(df1("DOJ").cast("Date"),"yyyy-MM-dd"))

      val df3 = df2.withColumn("DateDifference",datediff(df2("AddedDate"),df2("ReduceDays")))
          df3.show()
  }

}
