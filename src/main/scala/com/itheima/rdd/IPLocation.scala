package com.itheima.rdd

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  *
  */

object IPLocation {

  def ip2Long(ip: String): Long = {
    val split: Array[String] = ip.split("\\.");
    var ipNum = 0L;
    //遍历数组
    for(i<-split){
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum;
  }


  def binarySearch(ipNum: Long, ipValue: Array[(String, String, String, String)]): Int = {
    //定义一个开始下标
    var start = 0;
    var end = ipValue.length -1;
    while(start <= end){
      val middle = (start + end) /2;
      if(ipNum >= ipValue(middle)._1.toLong && ipNum <=ipValue(middle)._2.toLong){
        //return 直接退出while循环
        return middle;
      }
      if(ipNum < ipValue(middle)._1.toLong){
        end = middle -1;
      }
      if(ipNum > ipValue(middle)._2.toLong){
        start = middle +1;
      }
    }
    -1;
  }

  def main(args: Array[String]): Unit = {
    //1.创建sparkconf对象
    val master: SparkConf = new SparkConf().setAppName("Iplocation").setMaster("local[2]")
    //2.创建sparkcontext对象
    val sparkContext = new SparkContext(master)
    //设置日志打印级别
    sparkContext.setLogLevel("warn");
    //3.读取城市IP段,获取ip开始数字,ip结束数字,经纬度
    val city_ip_rdd: RDD[String] = sparkContext.textFile("F:\\云计算课视频\\Day73Scala\\scala_day02\\spark课程\\spark_day02\\资料\\服务器访问日志根据ip地址查找区域\\ip.txt");
    val map1: RDD[(String, String, String, String)] = city_ip_rdd.map(_.split("\\|")).map(x=>(x(2),x(3),x(x.length-2),x(x.length-1)))

    //把城市IP信息数据,通过广播变量下发到每一个worker节点
    val cityIPBroadcast: Broadcast[Array[(String, String, String, String)]] = sparkContext.broadcast(map1.collect())


    //获取位置IP地址
    val ip_rdd: RDD[String] = sparkContext.textFile("F:\\云计算课视频\\Day73Scala\\scala_day02\\spark课程\\spark_day02\\资料\\服务器访问日志根据ip地址查找区域\\20090121000132.394251.http.format").map(_.split("\\|")(1))

    //遍历cityIPBroadcast,获取每一个IP,转换成long类型的数字,通过二分查找匹配
    val result: RDD[((String, String), Int)] = ip_rdd.mapPartitions(iter => {
      //获取广播变量的值
      val ipValue: Array[(String, String, String, String)] = cityIPBroadcast.value
      //遍历迭代器.获取每一个IP地址
      iter.map(ip => {
        //把IP转换成Long类型数值
        val ipNum: Long = ip2Long(ip);
        //通过二分查找,查找long在数组中的下标
        val index: Int = binarySearch(ipNum, ipValue)
        val value: (String, String, String, String) = ipValue(index)
        ((value._3, value._4), 1);
      });
    })

    val finalResult: RDD[((String, String), Int)] = result.reduceByKey(_ + _);
    finalResult.foreachPartition(data =>{

      var conn : Connection = null;
      var ps : PreparedStatement = null;
      var sql = "insert into iplocation(longitude,latitude,total_count) values(?,?,?)";
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark","root","root");
      ps = conn.prepareStatement(sql);
      data.foreach(line=>{

        ps.setString(1,line._1._1);
        ps.setString(2,line._1._2)
        ps.setInt(3,line._2);
        ps.execute();
      })
      ps.close();
      conn.close();
    });
    sparkContext.stop();
  }
}

