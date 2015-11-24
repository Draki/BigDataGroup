package org.pfperez.matrices

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

object DistributedMultiplicationMapReduceStyle extends App {

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("Matrix multiplication")
    .set("matrixAPath", "input_files/matrices/A.txt")
    .set("matrixBPath", "input_files/matrices/B.txt")

  val sc = new SparkContext(conf)

  def loadMatrix(path: String): RDD[Array[Double]] = sc.textFile(path).map(_.split(" ").map(_.toDouble))

  def withIndex(matrix: RDD[Array[Double]]): RDD[Array[((Long, Long), Double)]] = {
    matrix.zipWithIndex map { row =>
      row._1.zipWithIndex map { case (v, j) => (row._2,j.toLong) -> v }
    }
  }

  def flattenedWithIndex(matrix: RDD[Array[Double]]): RDD[((Long, Long), Double)] = {
    matrix.zipWithIndex flatMap { row =>
      row._1.zipWithIndex map { case (v, j) => (row._2, j.toLong) -> v }
    }
  }

  def unflattened(withIndexMatrix: Map[(Long, Long), Double]): List[List[Double]] =
    (0L to withIndexMatrix.keys.map(_._1).max toList) map { i =>
      (0L to withIndexMatrix.keys.map(_._2).max toList) map { j =>
        withIndexMatrix(i, j)
      }
    }

  def transposeFlattened(fm: RDD[((Long, Long), Double)]) = fm map { case ((i, j),v) => ((j,i), v)}

  def printMatrix(m: Seq[Seq[_]]) = m.foreach(row => println(row mkString " "))

  ///////////////
  val A = flattenedWithIndex(loadMatrix(conf.get("matrixAPath")))
  val B = flattenedWithIndex(loadMatrix(conf.get("matrixBPath")))

  val fromA = A.map { case ((i, j), v: Double) => j -> ((i,j),v) }
  val fromB = B.map { case ((i, j), v: Double) => i -> ((i,j),v) }

  val cross = fromA.join(fromB)

  val res = cross map {
    case (k, (((ai,aj), va: Double), ((bi,bj), vb: Double))) => (ai, bj) -> va * vb
  } reduceByKey { case (a: Double, b: Double) => a + b }

  val resMatrix = unflattened(res.collect().toMap)

  printMatrix(resMatrix)

  sc.stop()

}
