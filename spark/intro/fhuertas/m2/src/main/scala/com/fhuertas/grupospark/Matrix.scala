package com.fhuertas.grupospark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Francisco Huertas on 21/11/15.
  */
object Matrix {
  def main (args: Array[String]) {
    val matrix1: Array[Array[Double]] = Array(Array(2.0,1.0,3.0), Array(1.0,0.0,5.0),Array(3.0,2.0,0.5))
    val matrix2: Array[Array[Double]] = Array(Array(8.6,1.3,4.5), Array(3.0,1.0,0.0),Array(2.5,1.7,1.1)).transpose
    //falta trasponer una matrix


    val conf = new SparkConf().setAppName("matrix").setMaster("local[2]")
    var sc:SparkContext = new SparkContext(conf)
    // memoria de 2n² -> 2n^3 distribución de los calculos O(n^2) tareas de O(n) operaciones cada uno.
    // 2n numeros en cada trabajo
    sc.parallelize(matrix1).cartesian(sc.parallelize(matrix2)).map(e=>(e._1,e._2).zipped.map(_*_).reduce(_+_))

    // With same format
    sc.parallelize(matrix1).cartesian(sc.parallelize(matrix2)).map(_.zipped.map(_*_).reduce(_+_)).zipWithIndex.map(e=>(e._2/3,Array(e._1))).reduceByKey(_++_).map(_._2).collect
    // incorrect in the map after zipWithIndex
//    sc.parallelize(matrix1).cartesian(sc.parallelize(matrix2)).map(_.zipped.map(_*_).reduce(_+_)).zipWithIndex.map((_._2/3,Array(_._1)).reduceByKey(_++_).map(_._2).collect

  }
}
