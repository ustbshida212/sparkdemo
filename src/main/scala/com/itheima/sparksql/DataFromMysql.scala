package com.itheima.sparksql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}


object DataFromMysql {
  def main(args: Array[String]): Unit = {
    //1.创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("FromMysql").master("local[2]").getOrCreate()

    //2.读取mysql表中的数据
    val properties = new Properties()
    properties.setProperty("user","root");
    properties.setProperty("password","root")
    val dataFrame: DataFrame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/spark", "iplocation", properties)
    dataFrame.createTempView("iplocation");

    sparkSession.sql("select * from iplocation").show();

    sparkSession.stop();
  }

}
