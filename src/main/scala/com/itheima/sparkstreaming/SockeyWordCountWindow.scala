package com.itheima.sparkstreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * 利用reduceByKeyAndWindow函数实现单词统计
  */

object SocketWordCountWindow {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("SocketWordCount").setMaster("local[2]")
    //10:52 sparkcontext对象创建
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //3.创建streamContext对象,第二个参数是批处理的时间间隔
    val streamingContext = new StreamingContext(sc, Seconds(5))

    //4.接收socket数据
    val socketTextStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("node03", 9999)

    //5.切分每一行获取所有的单词
    val mapResult: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_, 1));
    //需要3个参数:
    // 第一个:reduceFunc: (V, V) => V, 函数
    //第二个:nvReduceFunc: (V, V) => V,
    //第三个:windowDuration: Duration,窗口的长度
    val result: DStream[(String, Int)] = mapResult.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(10), Seconds(5))
    result.print();
    //6.开启流式计算
    streamingContext.start();
    //7.等待程序计算结束
    streamingContext.awaitTermination();

  }

}
