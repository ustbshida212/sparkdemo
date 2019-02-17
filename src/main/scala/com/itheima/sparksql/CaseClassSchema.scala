package com.itheima.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
  * 将RDD转换成DataFrame
  * 利用反射机制,声明样例类
  */

case class Person(id:Int,name:String,age:Int)
object CaseClassSchema {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder().appName("CaseClassSchema").master("local[2]").getOrCreate();
    //2.创建sparkContext对象
    val sc: SparkContext = spark.sparkContext;
    //3.读取数据创建RDD
    val rdd1: RDD[Array[String]] = sc.textFile("E:/person.txt").map(_.split(" "));
    val personRDD: RDD[Person] = rdd1.map(x => Person(x(0).toInt, x(1), x(2).toInt));
    //4.获取dataFrame
    //手动导入隐式转换
    import spark.implicits._
    val personDF: DataFrame = personRDD.toDF
    personDF.printSchema();
    personDF.show();//默认展示前20条数据,大于20个字符的字符串会被截取
    //----------DSL-------------

    personDF.show(1)
    personDF.head(1).foreach(println(_));
    personDF.first();
    println("-------------------")
    personDF.select(new Column("name")).show(1)

    personDF.groupBy("age").count().show()
    //----------------sql-----------
    personDF.createTempView("person");
    spark.sql("select * from person").show()

    //关闭
    sc.stop();
    spark.stop();



  }
}
