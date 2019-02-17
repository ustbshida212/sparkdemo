package com.itheima.sparkstreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SocketHotWord {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SocketHotWord").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn");
    val streamingContext = new StreamingContext(sc, Seconds(5));

    val socketTextStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("node03", 9999)
    val mapResult: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_, 1));

    val result: DStream[(String, Int)] = mapResult.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(10), Seconds(5))

    result.transform(rdd => {
      //按照单词次数降序排列
      val sortBy: RDD[(String, Int)] = rdd.sortBy(_._2, false)
      val take: Array[(String, Int)] = sortBy.take(3)
      take.foreach(println(_));
      rdd;
    })

    result.print();
    streamingContext.start();
    streamingContext.awaitTermination();

  }

}
