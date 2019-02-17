package com.itheima.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 将RDD转换成DataFrame
  * 使用StructType指定Schema
  */

object SparkSqlSchema {

  def main(args: Array[String]): Unit = {
    //1.创建sparkSession
    val spark: SparkSession = SparkSession.builder().appName("SparkSqlSchema").master("local[2]").getOrCreate()
    //2.得到RDD
    val mapRDD1: RDD[Array[String]] = spark.sparkContext.textFile("E:/person.txt").map(_.split(" "))
    //3.需要rdd1把Row关联
    val mapRow: RDD[Row] = mapRDD1.map(x => Row(x(0).toInt, x(1), x(2).toInt))
    //4.通过StrucType指定Schema
    val schema = StructType(
      //true表示是否可以为null (16:05)
        StructField("id", IntegerType, true) ::
        StructField("name", StringType, false) ::
        StructField("age", IntegerType, true) :: Nil)

    val createDataFrame: DataFrame = spark.createDataFrame(mapRow, schema)
    createDataFrame.show();

  spark.stop();

  }

}
