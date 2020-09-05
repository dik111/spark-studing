package com.example.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 通过读取内存集合中的数据，创建RDD
 */
object Spark05PartitionFile {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("Spark01_CreateRDD_mem").setMaster("local[2]")

    // 创建sparkcontext,该对象是提交spark App的入口
    val sc = new SparkContext(conf)

    // 创建一个集合对象
    val list1 = List(1,2,3,4)

    // 创建集合RDD 方式1
    val rdd = sc.textFile("/Users/yuwei1/Documents/scala/project/spark-studing/src/input/2.txt")

    rdd.saveAsTextFile("/Users/yuwei1/Documents/scala/project/spark-studing/src/output")

    //rdd.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
