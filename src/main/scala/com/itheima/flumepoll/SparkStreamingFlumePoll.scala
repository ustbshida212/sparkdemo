package com.itheima.flumepoll

import java.net.InetSocketAddress

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}

object SparkStreamingFlumePoll {
  def main(args: Array[String]): Unit = {

    //local[n],n必须要>1,接收数据是一个线程,数据处理是另外一个线程
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingFlumePoll").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("./fume-poll")
    val pollingStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, "node01", 8888)
    //获取Flume中数据,flume数据传输的最小单元是一个event:{"headers":xxxx,"body":xxx}

    //如果接收多台flume数据
//    FlumeUtils.createPollingStream(ssc,List(new InetSocketAddress("node01",8888),new InetSocketAddress("node02",8888)),StorageLevel.MEMORY_ONLY_SER);


    //获取event中body的数据
    val data: DStream[String] = pollingStream.map(x=>new String(x.event.getBody.array))

    val result: DStream[(String, Int)] = data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print();

    ssc.start();
    ssc.awaitTermination();

  }
}
