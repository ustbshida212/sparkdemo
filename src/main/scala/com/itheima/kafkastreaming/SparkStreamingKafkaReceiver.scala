package com.itheima.kafkastreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * 基于Receiver接收器使用了kafka高层次消费者api(消息的偏移量由zookeeper维护)
  * 可以解决数据不丢失的问题,但解决不了数据被处理且只被处理一次的问题.
  * 在这里由于更新偏移量到zk,如果没有成功,导致数据正常消费成功了,没有把这个消费的标识记录下来,最后导致数据的重复消费.
  */

object SparkStreamingKafkaReceiver {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SocketWordCount").setMaster("local[2]")
      //开启WAL日志,作用是:保证数据源的安全性,后期某些RDD分区数据丢失了,可以通过血统+checkpoint方式找回来
      .set("spark.streaming.receiver.writeAheadLog.enable","true");
    //10:52 sparkcontext对象创建
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //3.创建streamContext对象,第二个参数是批处理的时间间隔
    val ssc = new StreamingContext(sc, Seconds(5))
    //用于保存接收到的数据,实际工作可以写入到HDFS中
    ssc.checkpoint("./spark-receiver")

    //key:topic名称,value:表示一个receiver接收器使用几个线程消费topic数据
    val topics = Map("itcast" -> 1);


    /*val kafkaDstreamList = (1 to 3).map(
      x =>{
        //第一个是消息的key,第二个是消息的value
        val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,
          "node01:2181,node02:2181,node03:2181", "SparkReceiver", topics);
        kafkaDStream
      }
     );
        //通过ssc的union方法将三个KafkaDstream汇总
    val kafkaDStream: DStream[(String, String)] = ssc.union(kafkaDstreamList)
    */
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,
    "node01:2181,node02:2181,node03:2181", "SparkReceiver", topics);



    val data = kafkaDStream.map(_._2);
    //下面就是单词统计
    val reslut: DStream[(String, Int)] = data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    reslut.print();

    ssc.start();
    ssc.awaitTermination();

  }
}
