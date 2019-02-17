package com.itheima.flumepoll

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingFlumePush {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingFlumePush").setMaster("local[2]")

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("warn")
    val streamingContext = new StreamingContext(sparkContext,Seconds(5))

    //写localhost会报错
  val flumeDStreamReceiver: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createStream(streamingContext, "192.168.25.47", 8899)

    val data = flumeDStreamReceiver.map(x=>new String(x.event.getBody.array()))
    val result: DStream[(String, Int)] = data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print();

    streamingContext.start();
    streamingContext.awaitTermination();

  }

}
