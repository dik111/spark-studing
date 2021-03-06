package com.example.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 通过读取内存集合中的数据，创建RDD
 */
object Spark04CreateRDDMem {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("Spark01_CreateRDD_mem").setMaster("local[2]")

    // 创建sparkcontext,该对象是提交spark App的入口
    val sc = new SparkContext(conf)

    // 创建一个集合对象
    val list1 = List(1,2,3,4)

    // 创建集合RDD 方式1
    // val rdd = sc.parallelize(list1)

    // 创建RDD 方式2
    val rdd = sc.makeRDD(list1)

    rdd.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
