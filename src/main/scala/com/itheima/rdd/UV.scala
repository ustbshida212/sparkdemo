package com.itheima.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */
object UV {
  def main(args: Array[String]): Unit = {
    //1.创建sparkconf对象
    val master: SparkConf = new SparkConf().setAppName("UV").setMaster("local[2]")
    //2.创建sparkcontext对象
    val sparkContext = new SparkContext(master)
    //设置日志打印级别
    sparkContext.setLogLevel("warn");
    //3.读取日志文件
    val data: RDD[String] = sparkContext.textFile("F:\\云计算课视频\\Day73Scala\\scala_day02\\spark课程\\spark_day02\\资料\\运营商日志\\access.log")
    //4.通过IP地址获取PV,需要先切分,获取IP地址
    val count: Long = data.map(_.split(" ")(0)).distinct().count()
    println("UV:"+count);

    //5.关闭sparkContext
    sparkContext.stop();
  }
}
