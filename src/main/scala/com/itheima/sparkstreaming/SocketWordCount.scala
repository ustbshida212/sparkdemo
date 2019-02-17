package com.itheima.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 利用sparkStreaming接收socket数据,实现单词统计
  * 默认打印10条,超出的部分打印...
  */

object SocketWordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("SocketWordCount").setMaster("local[2]")
    //10:52 sparkcontext对象创建,如果用sparkConf对象创建,底层会自动创建sparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //3.创建streamContext对象,第二个参数是批处理的时间间隔
    val streamingContext = new StreamingContext(sc, Seconds(5))

    //4.接收socket数据
    val socketTextStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("node03", 9999)

    //5.切分每一行获取所有的单词
    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print();
    //6.开启流式计算
    streamingContext.start();
    //7.等待程序计算结束
    streamingContext.awaitTermination();
  }
}
