package com.example.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 对集合中的元素进行扁平化处理
 */
object Spark03FlatMap {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建sparkcontext,该对象是提交spark App的入口
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(List(1,2),List(3,4),List(5,6)),numSlices = 2)

    val newRdd = rdd.flatMap(datas => datas)

    newRdd.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
