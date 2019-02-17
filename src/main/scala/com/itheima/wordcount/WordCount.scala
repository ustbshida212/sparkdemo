package com.itheima.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf对象,设置applicationName和Master地址 local[2]表示本地采用2个线程运行程序
    val master: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    //2.创建SparkContext对象,所有spark程序的入口,内部会创建DAGScheduler和TaskScheduler
    val sc = new SparkContext(master);
    //3.读取本地文件,返回值是RDD,可以理解为一个集合,每行数据封装到一个Rdd string
    val file: RDD[String] = sc.textFile("E:/aa.txt")
    //4.按照空格切分,每个单词是一个RDD string
    val map: RDD[String] = file.flatMap(_.split(" "))
    //5.将每个单词数目记为1
    val map1: RDD[(String, Int)] = map.map((_, 1))
    //6.将相容单词出现的次数进行累加
    val result: RDD[(String, Int)] = map1.reduceByKey(_ + _)
      //sortBy()第二个参数默认是升序:ascending: Boolean = true,
    val sortedResult: RDD[(String, Int)] = result.sortBy(_._2)

    //7.收集然后打印
    val finalResult: Array[(String, Int)] = sortedResult.collect()
    finalResult.foreach(println(_));

    //8.关闭sparkContext对象
    sc.stop();
  }
}
