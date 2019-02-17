package com.itheima.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 利用sparkStreaming接收socket数据实现所有批次单词统计
  */

object SocketWordCountTotal {

  //currentValues表示当前批次中相同单词的出现的所有的1
//historyValues:表示每个单词在之前所有批次中出现的总次数
//Option类型:可以表示可能存在或不存在的值  存在Some 不存在表示None

 /* def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount =runningCount.getOrElse(0)+newValues.sum
    Some(newCount)
  }*/


  def updateFunction(currentValues:Seq[Int], historyValues:Option[Int]):Option[Int]= {

    val newCount = historyValues.getOrElse(0)+currentValues.sum
    Some(newCount)
  }

//  def updateFunction1: (scala.Seq[Int], _root_.scala.Option[Int]) => _root_.scala.Option[Int] = {
//    val newCount = historyValues.getOrElse(0)+currentValues.sum
//    Some(newCount)
//  }

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCountTotal").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("warn");

    val streamingContext = new StreamingContext(sparkContext,Seconds(5));

    //设置checkpoint目录,主要作用:是保存之前批次每一个单词出现的总次数
    streamingContext.checkpoint("./socket")

    val socketTextStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("node03", 9999)
    val map: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_, 1))
    val result: DStream[(String, Int)] = map.updateStateByKey(updateFunction)


    result.print();
    streamingContext.start();
    streamingContext.awaitTermination();

    val list = List(1,2,3,4)
  }
}
