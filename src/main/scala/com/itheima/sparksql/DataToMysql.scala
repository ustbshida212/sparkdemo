package com.itheima.sparksql

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

//利用sparksql把数据写入到mysql中
case class Student(id:Int,name:String,age:Int)
object DataToMysql {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.appName("ToMysql").master("local[2]").getOrCreate()

    val sparkContext: SparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("warn");
    val map: RDD[Array[String]] = sparkContext.textFile("E:/person.txt").map(_.split(" "))
    val studentRDD: RDD[Student] = map.map(x => Student(x(0).toInt,x(1),x(2).toInt))

    import sparkSession.implicits._
    val studentDF: DataFrame = studentRDD.toDF

    studentDF.createTempView("student");
    val result: DataFrame = sparkSession.sql("select * from student")

    val properties = new Properties();
    properties.setProperty("user","root")
    properties.setProperty("password","root")

    //overwrite:表示覆盖,如果表实现不存在,会自动创建
    //append:表示追加,如果表实现不存在,也会自动创建
    //ignore:表示忽略,如果表实现存在,那不会对数据进行插入,直接忽略
    //error:报错,默认选项,如果表存在,就报错

    result.write.jdbc("jdbc:mysql://localhost:3306/spark","student",properties);

    sparkContext.stop();
    sparkSession.stop();
  }
}
