package com.itheima.kafkastreaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * SparkStreaming整合kafka,使用消费者低级的API(消息偏移量不在zk里维护,由客户端自己维护)
  * 使用这套API去实现,使用了消费者的低级API,消息偏移量不再由zookeeper去保存
  * 关于偏移量保存问题,其实保存在任何地方都是可以的,前提是:
  * 1)SparkStreaming正常处理数据
  * 2)把处理的数据偏移量进行保存
  * 这两步操作需要放在同一个事务里
  *
  * 如果处理失败，只需要回滚事务就可以，所以不需要把接收到的数据再保存到WAL中
  *
  * 通过这种方式获得的DStream内部的RDD分区数跟Kafka中topic分区数是相等的.是一一映射的.
  * 通过Receiver不是一一映射的.
  *
  */

object SparkStreamingKafkaDirect {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingKafkaDirect").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn");
    val ssc = new StreamingContext(sc, Seconds(5))

    //设置checkpoint目录,用于保存消息偏移量
    ssc.checkpoint("./Spark-Direct");

    val kafkaParams = Map("bootstrap.servers" -> "node01:9092,node02:9092,node03:9092", "group.id" -> "spark-direct");

    val topics = Set("itcast");
    val inputDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)

    val result: DStream[(String, Int)] = inputDStream.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print();

    ssc.start();
    ssc.awaitTermination();

  }
}
