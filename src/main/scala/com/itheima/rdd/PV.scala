package com.itheima.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过spark实现点击流日志分析案例
  */
object PV {
  def main(args: Array[String]): Unit = {
    //1.创建sparkconf对象
    val master: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    //2.创建sparkcontext对象
    val sparkContext = new SparkContext(master)
    //设置日志打印级别
    sparkContext.setLogLevel("warn");
    //3.读取日志文件
    val data: RDD[String] = sparkContext.textFile("F:\\云计算课视频\\Day73Scala\\scala_day02\\spark课程\\spark_day02\\资料\\运营商日志\\access.log")
    //4.统计pv
    val pv: Long = data.count()

    println(pv);
    //5.关闭sparkContext
    sparkContext.stop();

  }
}
