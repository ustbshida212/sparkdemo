package com.itheima.sparkmllib_day04

/**
  * Created by zhao-chj on 2017/8/3.
  * 局部矩阵具有整数类型的行和列索引和双类型值，存储在单个机器上。
  * MLlib支持密集矩阵，其入口值以列主序列存储在单个双阵列中，稀疏矩阵的
  * 非零入口值以列主要顺序存储在压缩稀疏列（CSC）格式中。 例如，以下密集矩阵
  *
  * 局部矩阵的基类是Matrix，
  * 我们提供了两个实现：DenseMatrix和SparseMatrix。 我们建议使用Matrices中实现的工厂方法来创建本地矩阵。
  * 记住，MLlib中的局部矩阵以列主要顺序存储.
  */
object LocalMatrix {
  def main(args: Array[String]) {
    import org.apache.spark.mllib.linalg.{Matrix, Matrices}

    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    println(dm(2,0))
    // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    println(dm(2,1))
  }
}
