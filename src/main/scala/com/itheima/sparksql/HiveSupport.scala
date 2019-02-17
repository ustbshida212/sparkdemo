package com.itheima.sparksql

import org.apache.spark.sql.SparkSession

/**
  * 利用sparksql操作hivesql
  */

object HiveSupport {
  def main(args: Array[String]): Unit = {
    //1.创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("HiveSupport").master("local[2]")
      .enableHiveSupport().getOrCreate();
    //2.利用sparkSession操作hivesql语句
    //2.1创建hive表
    sparkSession.sql("create table user(id int,name string,age int) row format delimited fields terminated by ','");
    //2.2 记载数据到表中
    sparkSession.sql("load data local inpath './data/user.txt' into table user");

    //2.3简单查询
    sparkSession.sql("select * from user").show();
  }
}
