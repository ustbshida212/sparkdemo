package com.itheima.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 通过spark实现TopN
  */
object TopN {
  def main(args: Array[String]): Unit = {
    //1.创建sparkconf对象
    val master: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    //2.创建sparkcontext对象
    val sparkContext = new SparkContext(master)
    //设置日志打印级别
    sparkContext.setLogLevel("warn");
    //3.读取日志文件
    val data: RDD[String] = sparkContext.textFile("F:\\云计算课视频\\Day73Scala\\scala_day02\\spark课程\\spark_day02\\资料\\运营商日志\\access.log")
    //4.过滤出正常的数据,切分每一行获取所有的URL
    val result: RDD[(String, Int)] = data.filter(_.split(" ").length > 10).map(_.split(" ")(10)).filter(_ != "\"" + "-" + "\"").map((_, 1)).reduceByKey(_ + _).sortBy(_._2,false);
    //5.取出出现次数最多的前5位
    val take: Array[(String, Int)] = result.take(5)
    take.foreach(println(_));
  }
}
