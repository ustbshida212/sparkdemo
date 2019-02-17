package com.itheima.sparkmllib_day04

import org.apache.spark.mllib.linalg.{Vector, Vectors}

object LocalVector {
  def main(args: Array[String]) {
    val vd: Vector = Vectors.dense(2, 0, 6) //建立密集向量
    println(vd(2)) //打印稀疏向量第3个值
    val vs: Vector = Vectors.sparse(4, Array(0, 1, 2, 3), Array(9, 5, 2, 7)) //建立稀疏向量
    //第一个参数4代表输入数据的大小，一般要求大于等于输入的数据值，第二个参数是数据下标，第三个参数是数据值
    println(vs(2)) //打印稀疏向量第3个值
    //通过指定其非零条目来创建稀疏向量（1.0,0.0,3.0）
    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    println(sv2(0))
  }
}
