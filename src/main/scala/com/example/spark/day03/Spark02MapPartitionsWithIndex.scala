package com.example.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 以分区为单位，对RDD中的元素进行映射，并且带分区编号
 */
object Spark02MapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建sparkcontext,该对象是提交spark App的入口
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4),numSlices = 2)

    val newRdd = rdd.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    })
    newRdd.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
